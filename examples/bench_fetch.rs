//! Locked fetch throughput example.
//!
//! Against the null broker (`benchmarks/nullbroker`, KL09-07), `VERIFY=1`
//! checks every record's seeded ID, hash, key and headers: `SEED` must match
//! the server's `--fetch-seed`, `VERIFY_HEADERS` its `--fetch-headers`.
//! `ISOLATION` selects `read_uncommitted` (default) or `read_committed`.

use std::collections::HashMap;
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
    let count: u64 = std::env::var("COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8_000_000);
    let verify = std::env::var("VERIFY").is_ok_and(|v| v == "1");
    let seed: u64 = std::env::var("SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x5EED_0001);
    let verify_headers: usize = std::env::var("VERIFY_HEADERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let read_committed = std::env::var("ISOLATION").is_ok_and(|v| v == "read_committed");
    let max_wait_ms = std::env::var("MAX_WAIT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100i32);
    let max_bytes = std::env::var("MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16_777_216i32);
    let min_bytes = std::env::var("MIN_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1i32);

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
    let assigned = consumer.assignment().len();
    let start = Instant::now();
    let mut got = 0u64;
    let mut empty = 0u32;
    let mut expected: HashMap<i32, u64> = HashMap::new();
    let mut verified = 0u64;
    let mut mismatches = 0u64;
    let mut gaps = 0u64;
    while got < count {
        let recs = consumer.fetch().await?;
        if recs.is_empty() {
            empty += 1;
            if empty > 600 {
                return Err(partitionline::Error::Timeout);
            }
            continue;
        }
        empty = 0;
        if verify {
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
    let elapsed = start.elapsed().as_secs_f64();
    let rec_s = got as f64 / elapsed.max(1e-9);
    println!(
        "{{\"consumed\":{got},\"elapsed_s\":{elapsed:.6},\"consumed_rec_s\":{rec_s:.3},\"partitions\":{assigned},\"max_wait_ms\":{max_wait_ms},\"max_bytes\":{max_bytes},\"verified\":{verified},\"verify_mismatches\":{mismatches},\"verify_gaps\":{gaps}}}"
    );
    if verify && mismatches > 0 {
        return Err(partitionline::Error::protocol(format!(
            "{mismatches} verify mismatches of {got}"
        )));
    }
    Ok(())
}
