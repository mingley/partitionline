//! Partitionline fetch driver for the gateway gzip cell.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use partitionline::{Consumer, ConsumerConfig, IsolationLevel, TlsConfig, TopicPartition};
use sha2::{Digest, Sha256};

use crate::cells::Cell;
use crate::json1k;
use crate::rusage::ProcessRusage;
use crate::settings::{FetchSettings, TlsMode};

/// Outcome of one partitionline fetch drive.
pub struct FetchOutcome {
    /// Records offered by the broker (received).
    pub offered: u64,
    /// Records that verified against the history.
    pub verified_ids: u64,
    /// Duplicate ids.
    pub duplicates: u64,
    /// Payload / key / header / history mismatches.
    pub rejected: u64,
    /// Timed out before the target count.
    pub timed_out: u64,
    /// Timed-phase seconds.
    pub elapsed_s: f64,
    /// Effective fetch settings.
    pub settings: FetchSettings,
    /// Process rusage of this process.
    pub rusage: ProcessRusage,
}

/// Drive request.
pub struct FetchRequest<'a> {
    /// Cell.
    pub cell: Cell,
    /// Bootstrap.
    pub bootstrap: String,
    /// Topic.
    pub topic: String,
    /// Timed record count.
    pub count: u64,
    /// Explicit start offsets.
    pub assignment_offsets: &'a [i64],
    /// Produce-history id → SHA256 hex.
    pub expected_hashes: &'a std::collections::BTreeMap<u64, String>,
    /// One overall deadline.
    pub run_timeout: Duration,
    /// TLS mode.
    pub tls: TlsMode,
}

/// Run the partitionline side of the fetch cell.
pub async fn drive_partitionline_fetch(req: &FetchRequest<'_>) -> Result<FetchOutcome, String> {
    if req.assignment_offsets.len() != req.cell.partitions as usize {
        return Err("assignment_offsets must cover every partition".into());
    }
    let mut cfg = ConsumerConfig::bootstrap([req.bootstrap.clone()]);
    cfg.client_id = "gateway-partitionline-fetch".into();
    cfg.max_wait_ms = crate::cells::FETCH_MAX_WAIT_MS;
    cfg.min_bytes = crate::cells::FETCH_MIN_BYTES;
    cfg.max_bytes = crate::cells::FETCH_MAX_BYTES;
    cfg.max_partition_fetch_bytes = crate::cells::FETCH_PARTITION_BYTES;
    cfg.isolation_level = IsolationLevel::ReadUncommitted;
    cfg.enable_auto_commit = false;
    let tls_mode = apply_tls(&mut cfg, req.tls)?;
    let settings = FetchSettings {
        min_bytes: cfg.min_bytes,
        max_wait_ms: cfg.max_wait_ms,
        fetch_max_bytes: cfg.max_bytes,
        max_partition_fetch_bytes: cfg.max_partition_fetch_bytes,
        tls: tls_mode,
        isolation: "read_uncommitted".into(),
    };
    let cpu_origin = ProcessRusage::current();
    let mut consumer = Consumer::new(cfg)
        .await
        .map_err(|e| format!("consumer: {e}"))?;
    let starts: Vec<_> = req
        .assignment_offsets
        .iter()
        .enumerate()
        .map(|(partition, offset)| {
            (
                TopicPartition::new(
                    req.topic.clone(),
                    i32::try_from(partition).unwrap_or(i32::MAX),
                ),
                *offset,
            )
        })
        .collect();
    consumer
        .assign_many(starts)
        .await
        .map_err(|e| format!("assign: {e}"))?;
    let deadline = Instant::now()
        .checked_add(req.run_timeout)
        .ok_or_else(|| "run timeout overflow".to_string())?;
    let timed = Instant::now();
    let mut seen = BTreeSet::new();
    let mut offered = 0u64;
    let mut verified_ids = 0u64;
    let mut duplicates = 0u64;
    let mut rejected = 0u64;
    while verified_ids < req.count && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let batch = match tokio::time::timeout(remaining, consumer.fetch()).await {
            Ok(Ok(records)) => records,
            Ok(Err(error)) => return Err(format!("fetch: {error}")),
            Err(_) => break,
        };
        for rec in &batch {
            offered += 1;
            match verify_record(req, rec) {
                Verify::Ok(id) => {
                    if !seen.insert(id) {
                        duplicates += 1;
                    } else {
                        verified_ids += 1;
                    }
                }
                Verify::Bad => rejected += 1,
            }
            if verified_ids >= req.count {
                break;
            }
        }
    }
    let elapsed_s = timed.elapsed().as_secs_f64().max(1e-9);
    let _ = consumer.close().await;
    Ok(FetchOutcome {
        offered,
        verified_ids,
        duplicates,
        rejected,
        timed_out: u64::from(verified_ids < req.count),
        elapsed_s,
        settings,
        rusage: ProcessRusage::current().since(cpu_origin),
    })
}

enum Verify {
    Ok(u64),
    Bad,
}

fn verify_record(req: &FetchRequest<'_>, rec: &partitionline::FetchedRecord) -> Verify {
    let Some(key) = rec.key() else {
        return Verify::Bad;
    };
    if key.len() != 16 {
        return Verify::Bad;
    }
    let Ok(id) = key[..8].try_into().map(u64::from_be_bytes) else {
        return Verify::Bad;
    };
    let Some(expected_hash) = req.expected_hashes.get(&id) else {
        return Verify::Bad;
    };
    let Some(value) = rec.value() else {
        return Verify::Bad;
    };
    let actual_hash = format!("{:x}", Sha256::digest(value));
    let expected_value = json1k::value(json1k::SEED, usize::try_from(id).unwrap_or(usize::MAX));
    let expected_key = expected_key(json1k::SEED, id);
    let expected_header = json1k::header(json1k::SEED, id);
    let header_ok = rec
        .last_header(json1k::HEADER_KEY)
        .and_then(|header| header.value.as_deref())
        == Some(expected_header.as_slice());
    let partition_ok = rec.partition as u64 == id % req.cell.partitions as u64;
    if actual_hash != *expected_hash
        || value != expected_value.as_slice()
        || key != expected_key.as_slice()
        || !header_ok
        || !partition_ok
        || rec.topic != req.topic
    {
        return Verify::Bad;
    }
    Verify::Ok(id)
}

fn expected_key(seed: u64, id: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&id.to_be_bytes());
    let mut state = seed ^ id;
    key[8..].copy_from_slice(&json1k::splitmix64(&mut state).to_be_bytes());
    key
}

fn apply_tls(cfg: &mut ConsumerConfig, requested: TlsMode) -> Result<TlsMode, String> {
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

#[cfg(test)]
mod tests {
    use super::expected_key;
    use crate::json1k;

    #[test]
    fn expected_key_matches_splitmix_stream() {
        let key = expected_key(json1k::SEED, 1);
        assert_eq!(&key[..8], &1u64.to_be_bytes());
        let mut state = json1k::SEED ^ 1;
        assert_eq!(&key[8..], &json1k::splitmix64(&mut state).to_be_bytes());
    }
}
