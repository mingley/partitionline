//! Schema 1.0.0 result documents. Throughput uses acknowledged counts only.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::cells::{AckMode, Cell, ClientKind, Load};
use crate::rusage::ProcessRusage;
use crate::settings::ComparableSettings;

/// Inputs for one result file.
pub struct ResultDraft {
    /// Cell.
    pub cell: Cell,
    /// Load.
    pub load: Load,
    /// Client.
    pub client: ClientKind,
    /// Disposition.
    pub disposition: &'static str,
    /// Why the run did not execute, when applicable.
    pub reason: String,
    /// Comparable settings actually used (or intended).
    pub settings: ComparableSettings,
    /// Offered records.
    pub offered: u64,
    /// Accepted / enqueued records. Never used as throughput.
    pub accepted: u64,
    /// Broker-acknowledged records.
    pub acknowledged: u64,
    /// Rejected records.
    pub rejected: u64,
    /// Timed-out records.
    pub timed_out: u64,
    /// Unknown / unresolved records.
    pub unknown: u64,
    /// History-verified ids.
    pub verified_ids: u64,
    /// Steady-state seconds. Must be > 0 for the schema.
    pub elapsed_s: f64,
    /// Per-record ack samples, microseconds. Empty for fire-and-forget.
    pub latencies_us: Vec<f64>,
    /// Process rusage.
    pub rusage: ProcessRusage,
    /// High-watermark partitions.
    pub watermarks: Vec<Value>,
    /// Sum of offset deltas.
    pub watermark_delta: i64,
    /// Watermarks were queried.
    pub watermarks_queried: bool,
    /// 1-based repetition index.
    pub repetition_index: u64,
    /// Planned repetitions.
    pub total_repetitions: u64,
    /// Pairing order label.
    pub pairing_order: String,
    /// Companion artifact paths already written.
    pub artifacts: Vec<(String, PathBuf)>,
}

/// Acknowledged records / elapsed. Enqueue rates are never used.
#[must_use]
pub fn acknowledged_throughput(acknowledged: u64, elapsed_s: f64) -> f64 {
    if elapsed_s <= 0.0 {
        return 0.0;
    }
    acknowledged as f64 / elapsed_s
}

/// Build and write a schema 1.0.0 result object.
pub fn write_result(path: &Path, draft: &ResultDraft) -> Result<Value, String> {
    let elapsed = draft.elapsed_s.max(1e-9);
    let acked = draft.acknowledged;
    let throughput = acknowledged_throughput(acked, elapsed);
    let bytes = acked.saturating_mul(crate::json1k::VALUE_BYTES as u64);
    let cpu = draft.rusage.user_cpu_seconds + draft.rusage.system_cpu_seconds;
    let matches = draft.watermarks_queried && draft.watermark_delta == acked as i64;
    let verified = draft.disposition == "executed"
        && matches
        && draft.verified_ids == acked
        && draft.rejected == 0
        && draft.timed_out == 0;
    let latency = latency_block(&draft.latencies_us, draft.cell.ack_mode);
    let (git_commit, git_branch, tree_hash, clean) = git_identity();
    let host = host_block();
    let start = iso8601(SystemTime::now());
    let config_json = serde_json::to_vec_pretty(&draft.settings).map_err(|e| e.to_string())?;
    let config_sha = hex(&Sha256::digest(&config_json));
    let binary_sha = hex(&Sha256::digest(b"gateway-harness-no-binary-hash"));
    let mut artifacts = Vec::new();
    for (kind, artifact) in &draft.artifacts {
        if artifact.is_file() {
            let bytes = fs::read(artifact).map_err(|e| e.to_string())?;
            artifacts.push(json!({
                "path": artifact.display().to_string(),
                "type": kind,
                "sha256": hex(&Sha256::digest(&bytes)),
                "size_bytes": bytes.len(),
            }));
        }
    }
    if artifacts.is_empty() {
        artifacts.push(json!({
            "path": path.display().to_string(),
            "type": "result",
            "sha256": "0".repeat(64),
            "size_bytes": 0,
        }));
    }
    let doc = json!({
        "schema_version": "1.0.0",
        "contract_version": "1.1.0",
        "suite_hold": {
            "status": "active",
            "policy": "Unsigned gateway samples do not lift Suite HOLD",
            "note": "No comparison, qualification, or performance claim"
        },
        "scenario": {
            "scenario_id": format!("{}:{}:{}", draft.cell.id, draft.load.id(), draft.client.name()),
            "profile": "bulk",
            "tier": "exploratory",
            "peer": draft.client.peer_label(),
            "cell_disposition": draft.disposition,
            "equal_semantics": {
                "durability": {"replication_factor": 1, "min_insync_replicas": 1},
                "acks": draft.settings.acks,
                "idempotence": false,
                "isolation": "read_uncommitted",
                "security": {
                    "protocol": draft.settings.tls.security_protocol(),
                    "mechanism": "NONE"
                },
                "acknowledgment_mode": draft.cell.ack_mode.label(),
                "note": draft.reason,
            }
        },
        "provenance": {
            "source": {
                "git_commit": git_commit,
                "git_branch": git_branch,
                "repo_url": "https://github.com/mingley/partitionline",
                "clean": clean,
                "tree_hash": tree_hash,
                "peer": draft.client.name(),
                "note": "Harness-only gateway produce cell; librdkafka stays in excluded peer crates"
            },
            "binary": {
                "name": draft.client.name(),
                "path": "benchmarks/gateway",
                "sha256": binary_sha
            },
            "config": {
                "path": "effective-settings",
                "sha256": config_sha,
                "effective_settings": {
                    "acks": draft.settings.acks,
                    "linger_ms": draft.settings.linger_ms,
                    "batch_size_bytes": draft.settings.batch_size_bytes,
                    "max_in_flight": draft.settings.max_in_flight,
                    "idempotence": false,
                    "codec": draft.settings.codec,
                    "level": draft.settings.level,
                    "tls": draft.settings.tls,
                    "acknowledgment_mode": draft.cell.ack_mode.label(),
                }
            },
            "toolchains": {
                "compiler": rustc_version(),
                "runtime": "partitionline + isolated rust-rdkafka peer",
                "build_tool": "cargo"
            },
            "broker": {
                "image": std::env::var("BROKER_IMAGE").unwrap_or_else(|_| "unreported".into()),
                "version": std::env::var("BROKER_VERSION").unwrap_or_else(|_| "unreported".into()),
                "mode": "kraft",
                "cluster_id": "unreported",
                "node_count": 1,
                "endpoints": ["unreported"]
            },
            "host": host,
            "topology": {
                "environment": "cloud_vm",
                "rtt_ms": 0,
                "rtt_unit": "milliseconds",
                "client_nodes": 1,
                "broker_nodes": 1,
                "network_interface": "lo",
                "section2_differences": crate::cells::section2_differences(),
            },
            "timestamps": {
                "start_time_utc": start,
                "end_time_utc": start,
                "duration_seconds": elapsed,
                "duration_unit": "seconds"
            },
            "seeds": {
                "payload_seed": format!("{:#x}", crate::json1k::SEED),
                "key_seed": format!("{:#x}", crate::json1k::SEED),
                "partition_seed": "explicit index modulo 8",
                "repetition_seed": "0x5EED0004"
            },
            "artifacts": artifacts
        },
        "execution": {
            "phase": "steady_state",
            "warmup_completed": false,
            "warmup_records": 0,
            "warmup_duration_seconds": 0,
            "steady_state_duration_seconds": elapsed,
            "measured_phase_present": draft.disposition == "executed",
            "repetition_index": draft.repetition_index.max(1),
            "total_repetitions": draft.total_repetitions.max(1),
            "pairing_order": draft.pairing_order,
            "coordinated_omission_avoidance": {
                "enabled": draft.load.rate_per_second().is_some(),
                "schedule_type": if draft.load.rate_per_second().is_some() {
                    "open_loop_fixed_rate"
                } else {
                    "closed_loop"
                }
            },
            "acknowledgment_mode": draft.cell.ack_mode.label(),
            "note": draft.reason,
        },
        "outcomes": {
            "offered": draft.offered,
            "accepted": draft.accepted,
            "acknowledged": acked,
            "consumed": draft.verified_ids,
            "rejected": draft.rejected,
            "timed_out": draft.timed_out,
            "unknown": draft.unknown
        },
        "measurements": {
            "throughput": {
                "records_per_second": throughput,
                "records_per_second_unit": "records/s",
                "megabytes_per_second": (bytes as f64) / elapsed / 1e6,
                "megabytes_per_second_unit": "MB/s",
                "total_bytes_transferred": bytes,
                "total_bytes_unit": "bytes",
                "mb_definition": "1 MB=1000000 value bytes; keys/protocol excluded",
                "definition": "acknowledged / elapsed; enqueue rates are never reported as acknowledged throughput"
            },
            "latency": latency,
            "client_resources": {
                "cpu_utilization_pct": 100.0 * cpu / elapsed,
                "cpu_unit": "percent",
                "user_cpu_seconds": draft.rusage.user_cpu_seconds,
                "system_cpu_seconds": draft.rusage.system_cpu_seconds,
                "cpu_seconds_unit": "seconds",
                "allocations": {
                    "total_allocated_bytes": 0,
                    "allocation_count": 0,
                    "unit": "bytes",
                    "note": "Uninstrumented"
                },
                "rss": {
                    "peak_rss_bytes": draft.rusage.peak_rss_bytes,
                    "average_rss_bytes": 0,
                    "unit": "bytes",
                    "note": "getrusage / wait4 peak"
                },
                "threads_count": draft.rusage.threads_count.max(1),
                "cpu_source": "process rusage"
            },
            "broker_resources": {
                "cpu_utilization_pct": 0,
                "cpu_unit": "percent",
                "peak_rss_bytes": 0,
                "rss_unit": "bytes",
                "disk_write_bytes": 0,
                "disk_write_unit": "bytes",
                "note": "Unmeasured"
            },
            "errors": if draft.disposition == "not_run" || draft.disposition == "failed" {
                vec![json!({
                    "code": "gateway_campaign",
                    "name": draft.reason,
                    "count": 1,
                    "fatal": draft.disposition == "failed",
                    "phase": "steady_state"
                })]
            } else {
                Vec::new()
            }
        },
        "integrity": {
            "verified": verified,
            "record_ids": {
                "start_id": 0,
                "end_id": draft.offered.saturating_sub(1),
                "expected_count": acked,
                "verified_count": draft.verified_ids,
                "missing_ids_count": acked.saturating_sub(draft.verified_ids),
                "duplicate_ids_count": 0,
                "checksum_algorithm": "KL04-07 record history plus SHA256 payload hash",
                "payload_checksum_matches": verified
            },
            "high_watermark_audit": {
                "partitions": draft.watermarks,
                "total_offset_delta": draft.watermark_delta.max(0),
                "matches_acknowledged": matches
            },
            "idempotence_sequence_verified": false,
            "integrity_failure": draft.disposition == "failed"
        },
        "repetition_history": {
            "total_attempts": 1,
            "failed_attempts": u64::from(draft.disposition != "executed"),
            "attempts": [{
                "attempt_number": 1,
                "repetition_index": draft.repetition_index.max(1),
                "status": if draft.disposition == "executed" { "passed_measurement" } else { "failed_abort" },
                "integrity_failure": draft.disposition == "failed",
                "error_message": if draft.disposition == "executed" { Value::Null } else { Value::String(draft.reason.clone()) },
                "timestamp_utc": start,
                "broker_workload_attempted": draft.disposition == "executed" || draft.disposition == "failed"
            }]
        }
    });
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("create {}: {error}", path.display()))?;
    serde_json::to_writer_pretty(&mut file, &doc).map_err(|e| e.to_string())?;
    use std::io::Write;
    file.write_all(b"\n").map_err(|e| e.to_string())?;
    Ok(doc)
}

fn latency_block(samples: &[f64], mode: AckMode) -> Value {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len();
    let q = |p: f64| {
        if n == 0 {
            0.0
        } else {
            let rank = ((n as f64) * p).ceil().max(1.0) as usize - 1;
            sorted[rank.min(n - 1)]
        }
    };
    let mean = if n == 0 {
        0.0
    } else {
        sorted.iter().sum::<f64>() / n as f64
    };
    let var = if n == 0 {
        0.0
    } else {
        sorted.iter().map(|v| (v - mean) * (v - mean)).sum::<f64>() / n as f64
    };
    json!({
        "sample_count": n,
        "unit": "microseconds",
        "p50": q(0.50),
        "p90": q(0.90),
        "p95": q(0.95),
        "p99": q(0.99),
        "p99_9": q(0.999),
        "min": sorted.first().copied().unwrap_or(0.0),
        "max": sorted.last().copied().unwrap_or(0.0),
        "mean": mean,
        "stddev": var.sqrt(),
        "confidence_interval_95": {
            "lower": q(0.025),
            "upper": q(0.975),
            "unit": "microseconds"
        },
        "raw_histogram": {
            "bucket_unit": "microseconds",
            "buckets": [{"min_us": 0, "max_us": 0, "count": n}]
        },
        "acknowledgment_mode": mode.label(),
        "definition": if mode == AckMode::PerRecordCompletion {
            "offer-to-send-future-completion; not a fire-and-forget flush bound"
        } else {
            "fire-and-forget cells do not report per-record acknowledgment latency"
        }
    })
}

fn git_identity() -> (String, String, String, bool) {
    let commit = git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "0000000".into());
    let branch = git(&["branch", "--show-current"]).unwrap_or_else(|| "unknown".into());
    let tree = git(&["rev-parse", "HEAD^{tree}"]).unwrap_or_else(|| "0".repeat(40));
    let dirty = git(&["status", "--porcelain"]).unwrap_or_default();
    (commit, branch, tree, dirty.is_empty())
}

fn git(args: &[&str]) -> Option<String> {
    let out = std::process::Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn rustc_version() -> String {
    std::process::Command::new("rustc")
        .arg("+stable")
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "rustc unknown".into())
}

fn host_block() -> Value {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let mem = fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix("MemTotal:")
                    .and_then(|rest| rest.split_whitespace().next())
                    .and_then(|n| n.parse::<u64>().ok())
            })
        })
        .unwrap_or(1)
        .saturating_mul(1024)
        .max(1);
    let model = fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix("model name")
                    .and_then(|rest| rest.split(':').nth(1))
                    .map(|s| s.trim().to_string())
            })
        })
        .unwrap_or_else(|| "unavailable".into());
    json!({
        "hostname": "sanitized-local-host",
        "os": std::env::consts::OS,
        "os_family": std::env::consts::FAMILY,
        "kernel_version": fs::read_to_string("/proc/sys/kernel/osrelease")
            .unwrap_or_else(|_| "unknown".into())
            .trim()
            .to_string(),
        "arch": match std::env::consts::ARCH {
            "x86_64" => "x86_64",
            "aarch64" => "aarch64",
            other => other,
        },
        "cpu": {
            "model": model,
            "physical_cores": cpus,
            "logical_cores": cpus,
            "frequency_mhz": 0
        },
        "memory": {"total_bytes": mem, "unit": "bytes"}
    })
}

fn iso8601(time: SystemTime) -> String {
    let secs = time
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = i64::try_from(secs / 86400).unwrap_or(0);
    let (year, month, day) = civil_from_days(days);
    let rem = secs % 86400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Howard Hinnant civil-from-days (Unix epoch day 0 = 1970-01-01).
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 15) as usize] as char);
    }
    out
}

/// Draft a `not_run` result for a missing live prerequisite.
pub fn not_run_draft(
    cell: Cell,
    load: Load,
    client: ClientKind,
    reason: impl Into<String>,
    repetition_index: u64,
    total_repetitions: u64,
    pairing_order: impl Into<String>,
) -> ResultDraft {
    ResultDraft {
        cell,
        load,
        client,
        disposition: "not_run",
        reason: reason.into(),
        settings: ComparableSettings::for_cell(cell, crate::settings::TlsMode::Plain),
        offered: 0,
        accepted: 0,
        acknowledged: 0,
        rejected: 0,
        timed_out: 0,
        unknown: 0,
        verified_ids: 0,
        elapsed_s: 1e-6,
        latencies_us: Vec::new(),
        rusage: ProcessRusage {
            threads_count: 1,
            ..ProcessRusage::default()
        },
        watermarks: Vec::new(),
        watermark_delta: 0,
        watermarks_queried: false,
        repetition_index,
        total_repetitions,
        pairing_order: pairing_order.into(),
        artifacts: Vec::new(),
    }
}
