//! Fetch throughput diagnostics with independent deterministic record verification.

//!
//! Against the null broker (`benchmarks/nullbroker`, KL09-07), `VERIFY=1`
//! checks every record's seeded ID, hash, key and headers: `SEED` must match
//! the server's `--fetch-seed`, `VERIFY_HEADERS` its `--fetch-headers`.
//! `ISOLATION` selects `read_uncommitted` (default) or `read_committed`.

#[path = "common/bench_history.rs"]
mod history;

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use partitionline::{Consumer, ConsumerConfig, TlsConfig};

/// One step of splitmix64 (mirrors the null broker's synthetic log).
fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Verification hash for `(seed, partition, offset)`.
fn record_hash(seed: u64, partition: i32, offset: u64) -> u64 {
    // Partitions are never negative, matching the broker's `as u64`.
    let mixed = seed
        .wrapping_add(u64::from(u32::try_from(partition).unwrap_or(0)))
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(offset);
    splitmix64(mixed)
}

/// Big-endian readers that return `None` instead of panicking.
fn be_i32(bytes: Option<&[u8]>) -> Option<i32> {
    bytes
        .and_then(|b| b.try_into().ok())
        .map(i32::from_be_bytes)
}

fn be_u64(bytes: Option<&[u8]>) -> Option<u64> {
    bytes
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
}

fn be_u32(bytes: Option<&[u8]>) -> Option<u32> {
    bytes
        .and_then(|b| b.try_into().ok())
        .map(u32::from_be_bytes)
}

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "plbench".into());
    let count = history::setting("COUNT", 8_000_000u64)?;
    history::positive("COUNT", count)?;
    let verify = history::flag("VERIFY", false)?;
    let seed = history::setting("SEED", 0x5EED_0001u64)?;
    let payload_size = history::setting("PAYLOAD_BYTES", 100usize)?;
    let verify_headers = history::setting("VERIFY_HEADERS", 0usize)?;
    let isolation = std::env::var("ISOLATION").unwrap_or_else(|_| "read_uncommitted".into());
    if isolation != "read_uncommitted" && isolation != "read_committed" {
        return Err(partitionline::Error::protocol(
            "ISOLATION must be read_uncommitted or read_committed",
        ));
    }
    let read_committed = isolation == "read_committed";
    let max_wait_ms = history::setting("MAX_WAIT_MS", 100i32)?;
    let max_bytes = history::setting("MAX_BYTES", 16_777_216i32)?;
    let min_bytes = history::setting("MIN_BYTES", 1i32)?;
    if max_wait_ms < 0 || max_bytes <= 0 || min_bytes <= 0 || min_bytes > max_bytes {
        return Err(partitionline::Error::protocol(
            "invalid MAX_WAIT_MS/MAX_BYTES/MIN_BYTES",
        ));
    }
    let history_path = std::env::var("RECORD_HISTORY").ok();
    if history_path.is_some() && (verify || payload_size < history::MIN_PAYLOAD) {
        return Err(partitionline::Error::protocol("RECORD_HISTORY requires PAYLOAD_BYTES >= 24 and VERIFY=0 (VERIFY=1 is the null-broker fixture format)"));
    }
    let mut journal = history_path
        .as_deref()
        .map(history::Journal::create)
        .transpose()?;

    let mut cfg = ConsumerConfig::bootstrap([bootstrap]);
    cfg.max_wait_ms = max_wait_ms;
    cfg.max_bytes = max_bytes;
    cfg.min_bytes = min_bytes;
    if read_committed {
        cfg = cfg.isolation(partitionline::IsolationLevel::ReadCommitted);
    }
    if let Ok(ca_path) = std::env::var("TLS_CA_PEM") {
        let mut tls = TlsConfig {
            ca_pem: Some(tokio::fs::read(&ca_path).await.map_err(|e| {
                partitionline::Error::protocol(format!("read TLS_CA_PEM {ca_path}: {e}"))
            })?),
            ..TlsConfig::default()
        };
        if let Ok(name) = std::env::var("TLS_SERVER_NAME") {
            if !name.is_empty() {
                tls.server_name = Some(name);
            }
        }
        cfg.tls = Some(tls);
    }
    let mech = std::env::var("SASL_MECHANISM").unwrap_or_else(|_| "PLAIN".into());
    if mech == "OAUTHBEARER" {
        let principal = std::env::var("SASL_OAUTH_PRINCIPAL").unwrap_or_else(|_| "alice".into());
        cfg.sasl_oauthbearer = Some(principal);
    } else if let (Ok(user), Ok(pass)) = (
        std::env::var("SASL_USERNAME"),
        std::env::var("SASL_PASSWORD"),
    ) {
        match mech.as_str() {
            "SCRAM-SHA-256" => cfg.sasl_scram = Some((user, pass)),
            "SCRAM-SHA-512" => cfg.sasl_scram_sha512 = Some((user, pass)),
            "PLAIN" => cfg.sasl_plain = Some((user, pass)),
            other => {
                return Err(partitionline::Error::protocol(format!(
                    "unknown SASL_MECHANISM {other}"
                )));
            }
        }
    }

    let mut consumer = Consumer::new(cfg).await?;
    consumer.assign_topic(&topic, 0).await?;
    let mut assignment = consumer.assignment();
    assignment.sort_by_key(|tp| tp.partition());
    let assigned = assignment.len();
    if assigned == 0 {
        return Err(partitionline::Error::protocol("empty consumer assignment"));
    }
    let fence = if journal.is_some() {
        consumer.end_offsets(assignment.clone()).await?
    } else {
        Vec::new()
    };
    if let Some(ref mut journal) = journal {
        let offsets = fence
            .iter()
            .map(|(tp, offset)| format!("{{\"partition\":{},\"offset\":{offset}}}", tp.partition()))
            .collect::<Vec<_>>()
            .join(",");
        journal.line(&format!("{{\"kind\":\"config\",\"schema_version\":1,\"role\":\"consumer\",\"topic\":{},\"isolation_level\":{},\"seed\":{seed},\"payload_bytes\":{payload_size},\"partitions\":{assigned},\"count\":{count},\"end_offsets\":[{offsets}]}}", history::quote(&topic), history::quote(&isolation)))?;
        journal.checkpoint()?;
    }
    let start = Instant::now();
    let mut got = 0u64;
    let mut empty = 0u32;
    let mut expected: HashMap<i32, u64> = HashMap::new();
    let mut verified = 0u64;
    let mut mismatches = 0u64;
    let mut gaps = 0u64;
    let mut seen = HashSet::new();
    let mut last_id = HashMap::new();
    let mut failure = None;
    loop {
        if journal.is_some() {
            let positions = consumer.positions();
            if fence
                .iter()
                .all(|(tp, end)| positions.iter().any(|(p, offset)| p == tp && offset >= end))
            {
                break;
            }
        } else if got >= count {
            break;
        }
        let recs = match consumer.fetch().await {
            Ok(records) => records,
            Err(error) => {
                failure = Some(error);
                break;
            }
        };
        if recs.is_empty() {
            empty += 1;
            if empty > 600 {
                failure = Some(partitionline::Error::Timeout);
                break;
            }
            continue;
        }
        empty = 0;
        if let Some(ref mut journal) = journal {
            for rec in recs.as_ref() {
                let value = rec.value().unwrap_or(&[]);
                let identity = history::identity(value);
                let identity_text = identity
                    .map(|(s, id)| format!("{s:016x}:{id}"))
                    .unwrap_or_else(|| format!("malformed:{}:{}", rec.partition(), rec.offset()));
                journal.record(
                    &identity_text,
                    rec.topic(),
                    rec.partition(),
                    Some(rec.offset()),
                    rec.key(),
                    value,
                    "consume",
                    "accepted",
                )?;
                let mut ok = false;
                if let Some((record_seed, id)) = identity {
                    let partition_index =
                        usize::try_from(id % u64::try_from(assigned).unwrap_or(1)).unwrap_or(0);
                    let partition = assignment.get(partition_index).map(|tp| tp.partition());
                    ok = record_seed == seed
                        && id < count
                        && seen.insert(id)
                        && Some(rec.partition()) == partition
                        && rec.value() == Some(history::payload(seed, id, payload_size)?.as_ref())
                        && rec.key() == Some(history::key(seed, rec.partition()).as_ref());
                    if last_id
                        .get(&rec.partition())
                        .is_some_and(|previous| id <= *previous)
                    {
                        ok = false;
                    }
                    let _old = last_id.insert(rec.partition(), id);
                }
                if ok {
                    verified += 1;
                } else {
                    mismatches += 1;
                }
            }
            journal.checkpoint()?;
        } else if verify {
            for rec in recs.as_ref() {
                let partition = rec.partition();
                let offset = u64::try_from(rec.offset()).unwrap_or(u64::MAX);
                let hash = record_hash(seed, partition, offset);
                let mut ok = true;
                // Offset order: gaps allowed (aborted batches under
                // read_committed), repeats and regressions are not.
                let next = expected.entry(partition).or_insert(0);
                if offset < *next {
                    ok = false;
                } else {
                    if offset > *next {
                        gaps += 1;
                    }
                    *next = offset + 1;
                }
                // Key: partition + offset + hash fragment.
                match rec.key() {
                    Some(key) if key.len() == 16 => {
                        let p = be_i32(key.get(..4));
                        let o = be_u64(key.get(4..12));
                        let h = be_u32(key.get(12..16));
                        let frag = u32::try_from(hash & 0xffff_ffff).unwrap_or(u32::MAX);
                        ok &= p == Some(partition) && o == Some(offset) && h == Some(frag);
                    }
                    _ => ok = false,
                }
                // Value: ID + hash + splitmix filler.
                match rec.value() {
                    Some(value) if value.len() >= 16 => {
                        let id = be_u64(value.get(..8));
                        let h = be_u64(value.get(8..16));
                        ok &= id == Some(offset) && h == Some(hash);
                        let mut word = hash;
                        let mut rest = value.get(16..).unwrap_or(&[]);
                        while rest.len() >= 8 {
                            word = splitmix64(word);
                            ok &= rest.get(..8) == Some(word.to_be_bytes().as_slice());
                            rest = rest.get(8..).unwrap_or(&[]);
                        }
                        if !rest.is_empty() {
                            word = splitmix64(word);
                            let want = word.to_be_bytes();
                            ok &= Some(rest) == want.get(..rest.len());
                        }
                    }
                    _ => ok = false,
                }
                // Headers: `h{i}` keys with seeded 8-byte values.
                let headers = rec.headers();
                ok &= headers.len() == verify_headers;
                for (i, header) in headers.iter().enumerate() {
                    let want = format!("h{i}");
                    let hv = splitmix64(hash.wrapping_add(i as u64)).to_be_bytes();
                    ok &= header.key() == want && header.value() == Some(hv.as_slice());
                }
                if ok {
                    verified += 1;
                } else {
                    mismatches += 1;
                }
            }
        }
        got += recs.len() as u64;
    }
    if journal.is_some()
        && (got != count || u64::try_from(seen.len()).unwrap_or(0) != count || mismatches > 0)
        && failure.is_none()
    {
        failure = Some(partitionline::Error::protocol(format!("history integrity mismatch: consumed={got}, expected={count}, unique_ids={}, mismatches={mismatches}", seen.len())));
    }
    if verify && mismatches > 0 && failure.is_none() {
        failure = Some(partitionline::Error::protocol(format!(
            "{mismatches} verify mismatches of {got}"
        )));
    }
    let disposition = if failure.is_some() {
        "failed"
    } else {
        "executed"
    };
    if let Some(ref mut journal) = journal {
        journal.line(&format!("{{\"kind\":\"summary\",\"role\":\"consumer\",\"completed\":{},\"run_disposition\":\"{disposition}\",\"consumed\":{got},\"verified\":{verified},\"verify_mismatches\":{mismatches},\"unique_ids\":{}}}", failure.is_none(), seen.len()))?;
        journal.checkpoint()?;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let rec_s = got as f64 / elapsed.max(1e-9);
    println!(
        "{{\"consumed\":{got},\"elapsed_s\":{elapsed:.6},\"consumed_rec_s\":{rec_s:.3},\"partitions\":{assigned},\"max_wait_ms\":{max_wait_ms},\"max_bytes\":{max_bytes},\"verified\":{verified},\"verify_mismatches\":{mismatches},\"verify_gaps\":{gaps},\"record_history\":{},\"integrity_verified\":{},\"performance_claims_valid\":false,\"run_disposition\":\"{disposition}\"}}"
        , journal.is_some(), (journal.is_some() || verify) && failure.is_none()
    );
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}
