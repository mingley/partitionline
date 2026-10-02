//! Produce throughput diagnostics; RECORD_HISTORY enables deterministic ID/hash evidence.

#[path = "common/bench_history.rs"]
#[expect(
    dead_code,
    reason = "shared benchmark helper contains consumer-only envelope parsing"
)]
mod history;

use std::time::{Duration, Instant};

use bytes::Bytes;
use partitionline::{Compression, ProduceRecord, Producer, ProducerConfig, TlsConfig};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let bootstrap = std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "partitionline".into());
    let payload = history::setting("PAYLOAD_BYTES", 100usize)?;
    history::positive("PAYLOAD_BYTES", u64::try_from(payload).unwrap_or(u64::MAX))?;
    let warmup = Duration::from_secs(history::setting("WARMUP_SECS", 2u64)?);
    let measure = Duration::from_secs(history::setting("MEASURE_SECS", 5u64)?);
    let count: Option<u64> = history::optional_setting("COUNT")?;
    if let Some(count) = count {
        history::positive("COUNT", count)?;
    } else {
        history::positive("MEASURE_SECS", measure.as_secs())?;
    }
    let linger_ms = history::setting("LINGER_MS", 5u64)?;
    let acks = history::setting("ACKS", 1i16)?;
    if ![-1, 0, 1].contains(&acks) {
        return Err(partitionline::Error::protocol("ACKS must be -1, 0, or 1"));
    }
    let idempotent = history::flag("IDEMPOTENT", false)?;
    if idempotent && acks != -1 {
        return Err(partitionline::Error::protocol(
            "IDEMPOTENT=1 requires ACKS=-1",
        ));
    }
    let history_path = std::env::var("RECORD_HISTORY").ok();
    if history_path.is_some() && (payload < history::MIN_PAYLOAD || count.is_none()) {
        return Err(partitionline::Error::protocol(
            "RECORD_HISTORY requires COUNT and PAYLOAD_BYTES >= 24",
        ));
    }
    let seed = history::setting("SEED", 0x5EED_0001u64)?;
    let mut journal = history_path
        .as_deref()
        .map(history::Journal::create)
        .transpose()?;

    let mut cfg = ProducerConfig::bootstrap([bootstrap]);
    cfg.linger = Duration::from_millis(linger_ms);
    cfg.batch_records = 32_768;
    cfg.batch_bytes = 1_000_000;
    cfg.acks = acks;
    cfg.connections = history::setting("CONNECTIONS", 8usize)?;
    cfg.max_in_flight = history::setting("MAX_IN_FLIGHT", 16usize)?;
    history::positive(
        "CONNECTIONS",
        u64::try_from(cfg.connections).unwrap_or(u64::MAX),
    )?;
    history::positive(
        "MAX_IN_FLIGHT",
        u64::try_from(cfg.max_in_flight).unwrap_or(u64::MAX),
    )?;
    let compression =
        Compression::from_name(&std::env::var("COMPRESSION").unwrap_or_else(|_| "none".into()))?;
    cfg.compression = compression;
    if idempotent {
        cfg.enable_idempotence = true;
    }
    let tls_on = if let Ok(ca_path) = std::env::var("TLS_CA_PEM") {
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
        true
    } else {
        false
    };
    let mut scram_on = false;
    let mut scram512_on = false;
    let mut oauth_on = false;
    let mech = std::env::var("SASL_MECHANISM").unwrap_or_else(|_| "PLAIN".into());
    if mech == "OAUTHBEARER" {
        let principal = std::env::var("SASL_OAUTH_PRINCIPAL").unwrap_or_else(|_| "alice".into());
        cfg.sasl_oauthbearer = Some(principal);
        oauth_on = true;
    } else if let (Ok(user), Ok(pass)) = (
        std::env::var("SASL_USERNAME"),
        std::env::var("SASL_PASSWORD"),
    ) {
        match mech.as_str() {
            "SCRAM-SHA-256" => {
                cfg.sasl_scram = Some((user, pass));
                scram_on = true;
            }
            "SCRAM-SHA-512" => {
                cfg.sasl_scram_sha512 = Some((user, pass));
                scram512_on = true;
            }
            "PLAIN" => cfg.sasl_plain = Some((user, pass)),
            other => {
                return Err(partitionline::Error::protocol(format!(
                    "unknown SASL_MECHANISM {other}"
                )));
            }
        }
    }
    let producer = Producer::new(cfg).await?;
    let mut partitions: Vec<i32> = if journal.is_some() {
        producer
            .partitions_for(topic.clone())
            .await?
            .iter()
            .map(|p| p.partition())
            .collect()
    } else {
        Vec::new()
    };
    partitions.sort_unstable();
    if journal.is_some() && partitions.is_empty() {
        return Err(partitionline::Error::protocol(
            "empty topic partition metadata",
        ));
    }
    if let Some(ref mut journal) = journal {
        journal.line(&format!("{{\"kind\":\"config\",\"schema_version\":1,\"role\":\"producer\",\"topic\":{},\"acks\":{acks},\"idempotent\":{idempotent},\"transactional\":false,\"seed\":{seed},\"payload_bytes\":{payload},\"partitions\":{},\"count\":{}}}", history::quote(&topic), partitions.len(), count.unwrap_or(0)))?;
        journal.checkpoint()?;
    }
    let topic: std::sync::Arc<str> = topic.into();
    let value = Bytes::from(vec![b'x'; payload]);
    let mut next_id = 0u64;
    let mut queue_full_attempts = 0u64;

    #[expect(
        clippy::too_many_arguments,
        reason = "benchmark send carries explicit phase, generator, journal and acceptance state"
    )]
    async fn send_one(
        producer: &Producer,
        topic: &std::sync::Arc<str>,
        value: &Bytes,
        partitions: &[i32],
        seed: u64,
        payload_size: usize,
        next_id: &mut u64,
        journal: &mut Option<history::Journal>,
        phase: &str,
        queue_full_attempts: &mut u64,
    ) -> partitionline::Result<bool> {
        let mut record = ProduceRecord::to(topic.clone()).value(value.clone());
        let mut partition = 0;
        let id = *next_id;
        if journal.is_some() {
            let index =
                usize::try_from(id % u64::try_from(partitions.len()).unwrap_or(1)).unwrap_or(0);
            partition = *partitions
                .get(index)
                .ok_or_else(|| partitionline::Error::protocol("partition index"))?;
            record = record
                .partition(partition)
                .key(history::key(seed, partition))
                .value(history::payload(seed, id, payload_size)?);
        }
        let evidence = if journal.is_some() {
            Some(record.clone())
        } else {
            None
        };
        match producer.try_send(record) {
            Ok(()) => {
                if let (Some(journal), Some(record)) = (journal.as_mut(), evidence) {
                    journal.record(
                        &format!("{seed:016x}:{id}"),
                        topic,
                        partition,
                        None,
                        record.key.as_deref(),
                        record.value.as_deref().unwrap_or(&[]),
                        phase,
                        "accepted",
                    )?;
                }
                *next_id = next_id
                    .checked_add(1)
                    .ok_or_else(|| partitionline::Error::protocol("record ID overflow"))?;
                Ok(true)
            }
            Err(partitionline::Error::QueueFull) => {
                *queue_full_attempts = queue_full_attempts.saturating_add(1);
                if *queue_full_attempts % 32 == 0 {
                    tokio::task::yield_now().await;
                }
                Ok(false)
            }
            Err(error) => {
                if let (Some(journal), Some(record)) = (journal.as_mut(), evidence) {
                    journal.record(
                        &format!("{seed:016x}:{id}"),
                        topic,
                        partition,
                        None,
                        record.key.as_deref(),
                        record.value.as_deref().unwrap_or(&[]),
                        phase,
                        "failed",
                    )?;
                    journal.checkpoint()?;
                }
                Err(error)
            }
        }
    }

    // Keep the explicit loop here so journal and ID state remain shared between
    // warmup and measurement without hiding failures in a background task.
    let mut warmup_count = 0;
    let mut measured_count = 0;
    let mut elapsed = 0.0;
    let mut failure = None;
    for (phase, duration, limit) in [("warmup", warmup, None), ("measure", measure, count)] {
        let start = Instant::now();
        let deadline = start
            .checked_add(duration)
            .ok_or_else(|| partitionline::Error::protocol("measurement duration overflow"))?;
        let mut sent = 0u64;
        if limit.is_some() || !duration.is_zero() {
            loop {
                match send_one(
                    &producer,
                    &topic,
                    &value,
                    &partitions,
                    seed,
                    payload,
                    &mut next_id,
                    &mut journal,
                    phase,
                    &mut queue_full_attempts,
                )
                .await
                {
                    Ok(true) => sent += 1,
                    Ok(false) => {}
                    Err(error) => {
                        failure = Some(error);
                        break;
                    }
                }
                if let Some(n) = limit {
                    if sent >= n {
                        break;
                    }
                } else if Instant::now() >= deadline {
                    break;
                }
                if sent % 1024 == 0 {
                    if let Some(ref mut journal) = journal {
                        journal.checkpoint()?;
                    }
                }
            }
        }
        if failure.is_none() {
            if let Err(error) = producer.flush().await {
                failure = Some(error);
            }
        }
        if phase == "warmup" {
            warmup_count = sent;
        } else {
            measured_count = sent;
            elapsed = start.elapsed().as_secs_f64();
        }
        if failure.is_some() {
            break;
        }
    }
    // A teardown error invalidates the attempt before its completion journal
    // is sealed; successful delivery counters alone cannot erase that failure.
    let close = producer.clone().close().await;
    if failure.is_none() {
        failure = close.err();
    }
    let metrics = producer.metrics();
    let acknowledged_total = if acks == 0 { 0 } else { metrics.records_acked };
    let locally_completed_total = if acks == 0 { metrics.records_acked } else { 0 };
    let acknowledged = if acks == 0 {
        0
    } else {
        acknowledged_total.saturating_sub(warmup_count)
    };
    if failure.is_none()
        && (metrics.produce_errors > 0 || metrics.records_acked != next_id || measured_count == 0)
    {
        failure = Some(partitionline::Error::protocol(
            "delivery counters did not settle all accepted records",
        ));
    }
    let disposition = if failure.is_some() {
        "failed"
    } else {
        "executed"
    };
    if let Some(ref mut journal) = journal {
        journal.line(&format!("{{\"kind\":\"summary\",\"role\":\"producer\",\"completed\":{},\"run_disposition\":\"{disposition}\",\"accepted_total\":{next_id},\"acknowledged_total\":{acknowledged_total},\"locally_completed_total\":{locally_completed_total},\"warmup_records\":{warmup_count},\"measured_records\":{measured_count},\"produce_errors\":{},\"queue_full_attempts\":{queue_full_attempts}}}", failure.is_none(), metrics.produce_errors))?;
        journal.checkpoint()?;
    }
    let acked_rec_s = if acks == 0 {
        "null".into()
    } else {
        format!("{:.3}", acknowledged as f64 / elapsed.max(1e-9))
    };
    let accepted_rec_s = measured_count as f64 / elapsed.max(1e-9);
    println!(
        "{{\"acked\":{acknowledged},\"accepted\":{measured_count},\"locally_completed\":{},\"elapsed_s\":{elapsed:.6},\"acked_rec_s\":{acked_rec_s},\"accepted_rec_s\":{accepted_rec_s:.3},\"delivery_semantics\":\"{}\",\"payload_bytes\":{payload},\"acks\":{acks},\"linger_ms\":{linger_ms},\"compression\":\"{}\",\"idempotent\":{idempotent},\"tls\":{tls_on},\"scram\":{scram_on},\"scram512\":{scram512_on},\"oauthbearer\":{oauth_on},\"record_history\":{},\"integrity_verified\":false,\"performance_claims_valid\":false,\"run_disposition\":\"{disposition}\"}}",
        if acks == 0 { measured_count } else { 0 }, if acks == 0 { "local_complete" } else { "broker_ack" }, compression.as_str(), journal.is_some()
    );
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}
