//! Orchestrate the isolated rust-rdkafka binary without linking librdkafka.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde_json::json;

use crate::cells::{AckMode, Cell, Load};
use crate::rusage::ProcessRusage;
use crate::settings::{ComparableSettings, FetchSettings, TlsMode};

/// Broker and count settings shared by a rust-rdkafka produce run.
pub struct PeerWorkload<'a> {
    /// Bootstrap.
    pub bootstrap: &'a str,
    /// Topic.
    pub topic: &'a str,
    /// Timed records.
    pub count: u64,
    /// Warmup records.
    pub warmup: u64,
    /// TLS mode.
    pub tls: TlsMode,
    /// Overall deadline.
    pub run_timeout_ms: u64,
}

/// Config JSON the rust-rdkafka peer deserializes.
pub fn rust_peer_config(cell: Cell, load: Load, work: &PeerWorkload<'_>) -> serde_json::Value {
    json!({
        "bootstrap": work.bootstrap,
        "topic": work.topic,
        "count": work.count,
        "warmup": work.warmup,
        "payload_bytes": crate::json1k::VALUE_BYTES,
        "partitions": cell.partitions,
        "acks": cell.acks,
        "linger_ms": cell.linger_ms,
        "batch_size_bytes": cell.batch_size_bytes,
        "batch_num_messages": 32768,
        "max_in_flight": cell.max_in_flight,
        "queue_max_messages": 1_000_000,
        "queue_max_kbytes": 32768,
        "delivery_timeout_ms": 30_000,
        "flush_timeout_ms": 35_000,
        "run_timeout_ms": work.run_timeout_ms,
        "consume_timeout_ms": 30_000,
        "record_seed": crate::json1k::SEED,
        "latency_samples": work.count,
        "idempotence": false,
        "compression": cell.codec,
        "compression_level": cell.level,
        "isolation_level": "read_uncommitted",
        "payload_mode": "json1k",
        "key_mode": "id",
        "header_mode": "gw8",
        "target_rate_per_s": load.rate_per_second().unwrap_or(0),
        "security_protocol": work.tls.security_protocol(),
        "sasl_mechanism": "",
        "acknowledgment_mode": cell.ack_mode.label(),
        "fetch_min_bytes": crate::cells::FETCH_MIN_BYTES,
        "fetch_wait_max_ms": crate::cells::FETCH_MAX_WAIT_MS,
        "fetch_max_bytes": crate::cells::FETCH_MAX_BYTES,
        "max_partition_fetch_bytes": crate::cells::FETCH_PARTITION_BYTES,
        "assignment_offsets": [],
    })
}

/// Broker and assignment settings for a rust-rdkafka consume run.
pub struct FetchWorkload<'a> {
    /// Bootstrap.
    pub bootstrap: &'a str,
    /// Topic.
    pub topic: &'a str,
    /// Records to verify.
    pub count: u64,
    /// Explicit start offsets, one per partition.
    pub assignment_offsets: &'a [i64],
    /// TLS mode.
    pub tls: TlsMode,
    /// Overall deadline.
    pub run_timeout_ms: u64,
}

/// Config JSON the rust-rdkafka peer deserializes for consume.
pub fn rust_peer_fetch_config(cell: Cell, work: &FetchWorkload<'_>) -> serde_json::Value {
    json!({
        "bootstrap": work.bootstrap,
        "topic": work.topic,
        "count": work.count,
        "warmup": 0,
        "payload_bytes": crate::json1k::VALUE_BYTES,
        "partitions": cell.partitions,
        "acks": cell.acks,
        "linger_ms": cell.linger_ms,
        "batch_size_bytes": cell.batch_size_bytes,
        "batch_num_messages": 32768,
        "max_in_flight": cell.max_in_flight,
        "queue_max_messages": 1_000_000,
        "queue_max_kbytes": 32768,
        "delivery_timeout_ms": 30_000,
        "flush_timeout_ms": 35_000,
        "run_timeout_ms": work.run_timeout_ms,
        "consume_timeout_ms": work.run_timeout_ms,
        "record_seed": crate::json1k::SEED,
        "latency_samples": work.count,
        "idempotence": false,
        "compression": cell.codec,
        "compression_level": cell.level,
        "isolation_level": "read_uncommitted",
        "payload_mode": "json1k",
        "key_mode": "id",
        "header_mode": "gw8",
        "target_rate_per_s": 0,
        "security_protocol": work.tls.security_protocol(),
        "sasl_mechanism": "",
        "acknowledgment_mode": cell.ack_mode.label(),
        "fetch_min_bytes": crate::cells::FETCH_MIN_BYTES,
        "fetch_wait_max_ms": crate::cells::FETCH_MAX_WAIT_MS,
        "fetch_max_bytes": crate::cells::FETCH_MAX_BYTES,
        "max_partition_fetch_bytes": crate::cells::FETCH_PARTITION_BYTES,
        "assignment_offsets": work.assignment_offsets,
    })
}

/// Locate the rust-rdkafka peer binary.
pub fn rust_peer_binary() -> Result<PathBuf, String> {
    if let Ok(path) = std::env::var("GATEWAY_RUST_PEER_BIN") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
        return Err(format!(
            "GATEWAY_RUST_PEER_BIN is not a file: {}",
            path.display()
        ));
    }
    Err("GATEWAY_RUST_PEER_BIN is unset; rust-rdkafka stays in the excluded peer crate".into())
}

/// Spawn `emit-config` and parse the peer's effective configuration.
pub fn emit_peer_config(
    binary: &Path,
    config: &serde_json::Value,
    work: &Path,
) -> Result<ComparableSettings, String> {
    let config_path = work.join("peer-input.json");
    write_new(&config_path, config)?;
    let native =
        std::env::var("GATEWAY_RUST_PEER_NATIVE_SHA256").unwrap_or_else(|_| "0".repeat(64));
    let output = Command::new(binary)
        .arg("emit-config")
        .env("RUST_PEER_CONFIG", &config_path)
        .env("RUST_PEER_NATIVE_SHA256", native)
        .output()
        .map_err(|error| format!("spawn rust peer emit-config: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "rust peer emit-config failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let dump: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("rust peer emit-config JSON: {error}"))?;
    let effective = dump
        .get("effective_config")
        .cloned()
        .ok_or_else(|| "rust peer emit-config missing effective_config".to_string())?;
    ComparableSettings::from_effective(&effective)
}

/// One rust-rdkafka produce run reaped with wait4 for CPU.
pub struct PeerRun {
    /// Process rusage of the peer child only.
    pub rusage: ProcessRusage,
    /// Raw driver JSON, if written.
    pub raw: serde_json::Value,
    /// Child exit code.
    pub exit_code: i32,
}

/// Spawn `emit-consumer-config` and parse the peer's fetch knobs.
pub fn emit_peer_consumer_config(
    binary: &Path,
    config: &serde_json::Value,
    work: &Path,
) -> Result<FetchSettings, String> {
    let config_path = work.join("peer-input.json");
    write_new(&config_path, config)?;
    let native =
        std::env::var("GATEWAY_RUST_PEER_NATIVE_SHA256").unwrap_or_else(|_| "0".repeat(64));
    let output = Command::new(binary)
        .arg("emit-consumer-config")
        .env("RUST_PEER_CONFIG", &config_path)
        .env("RUST_PEER_NATIVE_SHA256", native)
        .output()
        .map_err(|error| format!("spawn rust peer emit-consumer-config: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "rust peer emit-consumer-config failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let dump: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("rust peer emit-consumer-config JSON: {error}"))?;
    let effective = dump
        .get("effective_config")
        .cloned()
        .ok_or_else(|| "rust peer emit-consumer-config missing effective_config".to_string())?;
    FetchSettings::from_effective(&effective)
}

/// Spawn `consume` and collect the raw artifact plus child rusage.
pub fn run_peer_consume(
    binary: &Path,
    config: &serde_json::Value,
    work: &Path,
) -> Result<PeerRun, String> {
    let config_path = work.join("peer-input.json");
    write_new(&config_path, config)?;
    let raw_path = work.join("peer.raw.json");
    let history = work.join("peer.attempts.jsonl");
    let native =
        std::env::var("GATEWAY_RUST_PEER_NATIVE_SHA256").unwrap_or_else(|_| "0".repeat(64));
    let log = fs::File::create(work.join("peer.stderr.log"))
        .map_err(|error| format!("peer stderr: {error}"))?;
    let child = Command::new(binary)
        .arg("consume")
        .env("RUST_PEER_CONFIG", &config_path)
        .env("RUST_PEER_NATIVE_SHA256", native)
        .env("RUST_PEER_RAW", &raw_path)
        .env("RUST_PEER_HISTORY", &history)
        .stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?))
        .stderr(Stdio::from(log))
        .spawn()
        .map_err(|error| format!("spawn rust peer consume: {error}"))?;
    let pid = child.id();
    let (exit_code, rusage) = ProcessRusage::wait4(pid)?;
    std::mem::forget(child);
    let raw = if raw_path.is_file() {
        serde_json::from_slice(&fs::read(&raw_path).map_err(|e| e.to_string())?)
            .map_err(|error| format!("peer raw JSON: {error}"))?
    } else {
        json!({})
    };
    Ok(PeerRun {
        rusage,
        raw,
        exit_code,
    })
}

/// Count unique verified record ids in a consume history file.
pub fn count_verified_history(path: &Path) -> u64 {
    let Ok(text) = fs::read_to_string(path) else {
        return 0;
    };
    let mut ids = std::collections::BTreeSet::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let Ok(row) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if row.get("status").and_then(serde_json::Value::as_str) != Some("verified") {
            continue;
        }
        if let Some(id) = row.get("id").and_then(serde_json::Value::as_str) {
            ids.insert(id.to_string());
        } else if let Some(id) = row.get("id").and_then(serde_json::Value::as_u64) {
            ids.insert(id.to_string());
        }
    }
    ids.len() as u64
}

/// Load acked produce-history hashes keyed by record id.
pub fn load_acked_hashes(path: &Path) -> Result<std::collections::BTreeMap<u64, String>, String> {
    let text = fs::read_to_string(path).map_err(|e| format!("history {}: {e}", path.display()))?;
    let mut hashes = std::collections::BTreeMap::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let row: serde_json::Value = serde_json::from_str(line).map_err(|e| e.to_string())?;
        if row.get("status").and_then(serde_json::Value::as_str) != Some("acked") {
            continue;
        }
        let id = row
            .get("id")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .ok_or_else(|| "history row missing id".to_string())?;
        let hash = row
            .get("payload_hash")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "history row missing payload_hash".to_string())?
            .to_ascii_lowercase();
        hashes.insert(id, hash);
    }
    Ok(hashes)
}

/// Spawn `produce` and collect the raw artifact plus child rusage.
pub fn run_peer_produce(
    binary: &Path,
    config: &serde_json::Value,
    work: &Path,
) -> Result<PeerRun, String> {
    let config_path = work.join("peer-input.json");
    write_new(&config_path, config)?;
    let raw_path = work.join("peer.raw.json");
    let samples = work.join("peer.samples.csv");
    let history = work.join("peer.attempts.jsonl");
    let native =
        std::env::var("GATEWAY_RUST_PEER_NATIVE_SHA256").unwrap_or_else(|_| "0".repeat(64));
    let log = fs::File::create(work.join("peer.stderr.log"))
        .map_err(|error| format!("peer stderr: {error}"))?;
    let child = Command::new(binary)
        .arg("produce")
        .env("RUST_PEER_CONFIG", &config_path)
        .env("RUST_PEER_NATIVE_SHA256", native)
        .env("RUST_PEER_RAW", &raw_path)
        .env("RUST_PEER_SAMPLES", &samples)
        .env("RUST_PEER_HISTORY", &history)
        .stdout(Stdio::from(log.try_clone().map_err(|e| e.to_string())?))
        .stderr(Stdio::from(log))
        .spawn()
        .map_err(|error| format!("spawn rust peer produce: {error}"))?;
    let pid = child.id();
    let (exit_code, rusage) = ProcessRusage::wait4(pid)?;
    std::mem::forget(child);
    let raw = if raw_path.is_file() {
        serde_json::from_slice(&fs::read(&raw_path).map_err(|e| e.to_string())?)
            .map_err(|error| format!("peer raw JSON: {error}"))?
    } else {
        json!({})
    };
    Ok(PeerRun {
        rusage,
        raw,
        exit_code,
    })
}

/// Write `value` once. Emit-config and produce share `peer-input.json`;
/// a second create_new would fail with EEXIST and skip the live peer run.
fn write_new(path: &Path, value: &serde_json::Value) -> Result<(), String> {
    if path.is_file() {
        return Ok(());
    }
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("create {}: {error}", path.display()))?;
    serde_json::to_writer_pretty(&mut file, value).map_err(|e| e.to_string())?;
    use std::io::Write;
    file.write_all(b"\n").map_err(|e| e.to_string())
}

/// Count unique acked record ids in a KL04-07 rust-peer attempts file.
pub fn count_acked_history(path: &Path) -> u64 {
    let Ok(text) = fs::read_to_string(path) else {
        return 0;
    };
    let mut ids = std::collections::BTreeSet::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let Ok(row) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if row.get("status").and_then(serde_json::Value::as_str) != Some("acked") {
            continue;
        }
        if let Some(id) = row.get("id").and_then(serde_json::Value::as_str) {
            ids.insert(id.to_string());
        } else if let Some(id) = row.get("id").and_then(serde_json::Value::as_u64) {
            ids.insert(id.to_string());
        }
    }
    ids.len() as u64
}

/// Load rust-peer `record_id,latency_us` samples for per-record cells.
pub fn read_latency_samples(path: &Path) -> Vec<f64> {
    let Ok(text) = fs::read_to_string(path) else {
        return Vec::new();
    };
    text.lines()
        .skip(1)
        .filter_map(|line| line.split(',').nth(1)?.parse().ok())
        .collect()
}

/// Acknowledgment mode the rust peer used for this cell.
#[must_use]
pub fn peer_ack_mode(cell: Cell) -> AckMode {
    cell.ack_mode
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn acked_history_counts_unique_ids() {
        let dir = std::env::temp_dir().join(format!(
            "gateway-peer-hist-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let history = dir.join("peer.attempts.jsonl");
        std::fs::write(
            &history,
            "{\"id\":\"0\",\"status\":\"acked\"}\n{\"id\":\"1\",\"status\":\"failed\"}\n{\"id\":\"0\",\"status\":\"acked\"}\n{\"id\":\"2\",\"status\":\"acked\"}\n",
        )
        .unwrap();
        assert_eq!(count_acked_history(&history), 2);
        let verified = dir.join("consume.jsonl");
        std::fs::write(
            &verified,
            "{\"id\":\"0\",\"status\":\"verified\"}\n{\"id\":\"1\",\"status\":\"verified\"}\n{\"id\":\"1\",\"status\":\"verified\"}\n",
        )
        .unwrap();
        assert_eq!(count_verified_history(&verified), 2);
        let samples = dir.join("peer.samples.csv");
        std::fs::write(&samples, "record_id,latency_us\n0,12.5\n1,8\n").unwrap();
        assert_eq!(read_latency_samples(&samples), vec![12.5, 8.0]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn emit_and_produce_reuse_the_same_peer_input() {
        let dir = std::env::temp_dir().join(format!(
            "gateway-peer-input-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("peer-input.json");
        let value = json!({"count": 1});
        write_new(&path, &value).unwrap();
        write_new(&path, &json!({"count": 2})).unwrap();
        let stored: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(stored["count"], 1);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
