//! Produce throughput diagnostics; RECORD_HISTORY enables deterministic ID/hash evidence.

#[path = "common/bench_history.rs"]
#[expect(
    dead_code,
    reason = "shared benchmark helper contains consumer-only envelope parsing"
)]
mod history;

#[path = "common/bench_produce_settings.rs"]
mod settings;

use std::time::Instant;

use bytes::Bytes;
use partitionline::{ProduceRecord, Producer, TlsConfig};
use settings::{KeyMode, PayloadMode, Settings};

#[tokio::main]
async fn main() -> partitionline::Result<()> {
    let settings = Settings::from_env()?;
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if !arguments.is_empty() && arguments != ["--print-config"] {
        return Err(partitionline::Error::protocol(
            "usage: bench_produce [--print-config]",
        ));
    }
    let effective_settings = settings.effective_json();
    if arguments == ["--print-config"] {
        println!("{{\"effective_settings\":{effective_settings}}}");
        return Ok(());
    }
    let topic = settings.topic.clone();
    let payload = settings.payload;
    let warmup = settings.warmup;
    let measure = settings.measure;
    let count = settings.count;
    let linger_ms = settings.producer.linger.as_millis();
    let acks = settings.producer.acks;
    let idempotent = settings.producer.enable_idempotence;
    let compression = settings.producer.compression;
    let seed = settings.seed;
    let mut journal = settings
        .history_path
        .as_deref()
        .map(history::Journal::create)
        .transpose()?;
    let mut cfg = settings.producer.clone();
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
    let mut partitions: Vec<i32> =
        if journal.is_some() || settings.key_mode == KeyMode::Id || settings.partitions.is_some() {
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
    if (journal.is_some() || settings.key_mode == KeyMode::Id || settings.partitions.is_some())
        && partitions.is_empty()
    {
        return Err(partitionline::Error::protocol(
            "empty topic partition metadata",
        ));
    }
    if settings
        .partitions
        .is_some_and(|expected| expected != partitions.len())
    {
        return Err(partitionline::Error::protocol(
            "PARTITIONS differs from actual topic metadata",
        ));
    }
    if let Some(ref mut journal) = journal {
        journal.line(&format!("{{\"kind\":\"config\",\"schema_version\":1,\"role\":\"producer\",\"topic\":{},\"acks\":{acks},\"idempotent\":{idempotent},\"transactional\":false,\"seed\":{seed},\"payload_bytes\":{payload},\"partitions\":{},\"count\":{}}}", history::quote(&topic), partitions.len(), count.unwrap_or(0)))?;
        journal.checkpoint()?;
    }
    let topic: std::sync::Arc<str> = topic.into();
    let value = if settings.payload_mode == PayloadMode::ConstantX {
        Bytes::from(vec![b'x'; payload])
    } else {
        Bytes::new()
    };
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
        settings: &Settings,
        phase_id: u64,
        pending: &mut Option<ProduceRecord>,
        next_id: &mut u64,
        journal: &mut Option<history::Journal>,
        phase: &str,
        queue_full_attempts: &mut u64,
    ) -> partitionline::Result<bool> {
        let id = *next_id;
        let generator_id = if journal.is_some() { id } else { phase_id };
        let partition = if partitions.is_empty() {
            0
        } else {
            *partitions
                .get(usize::try_from(generator_id % partitions.len() as u64).unwrap_or(0))
                .ok_or_else(|| partitionline::Error::protocol("partition index"))?
        };
        if pending.is_none() {
            let payload = match settings.payload_mode {
                PayloadMode::ConstantX => value.clone(),
                PayloadMode::Seeded => settings::seeded_payload(seed, generator_id, payload_size),
                PayloadMode::History => history::payload(seed, id, payload_size)?,
            };
            let mut record = ProduceRecord::to(topic.clone()).value(payload);
            if !partitions.is_empty() {
                record = record.partition(partition);
            }
            record = match settings.key_mode {
                KeyMode::None => record,
                KeyMode::Id => record.key(settings::id_key(seed, generator_id)),
                KeyMode::History => record.key(history::key(seed, partition)),
            };
            *pending = Some(record);
        }
        let record = pending
            .as_ref()
            .ok_or_else(|| partitionline::Error::protocol("missing benchmark record"))?
            .clone();
        let evidence = if journal.is_some() {
            Some(record.clone())
        } else {
            None
        };
        match producer.try_send(record) {
            Ok(()) => {
                *pending = None;
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
                if queue_full_attempts.is_multiple_of(32) {
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
    let mut warmup_elapsed = 0.0;
    let mut elapsed = 0.0;
    let mut failure = None;
    for (phase, duration, limit) in [
        ("warmup", warmup, settings.warmup_records),
        ("measure", measure, count),
    ] {
        let mut pending_record = None;
        let start = Instant::now();
        let deadline = start
            .checked_add(duration)
            .ok_or_else(|| partitionline::Error::protocol("measurement duration overflow"))?;
        let mut sent = 0u64;
        if limit.is_some_and(|count| count > 0) || !duration.is_zero() {
            loop {
                match send_one(
                    &producer,
                    &topic,
                    &value,
                    &partitions,
                    seed,
                    payload,
                    &settings,
                    sent,
                    &mut pending_record,
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
                if start.elapsed() >= settings.run_timeout {
                    failure = Some(partitionline::Error::protocol("benchmark run timeout"));
                    break;
                }
                if let Some(n) = limit {
                    if sent >= n && (phase != "warmup" || Instant::now() >= deadline) {
                        break;
                    }
                } else if Instant::now() >= deadline {
                    break;
                }
                if sent.is_multiple_of(1024) {
                    if let Some(ref mut journal) = journal {
                        journal.checkpoint()?;
                    }
                }
            }
        }
        if failure.is_none() {
            if let Err(error) = tokio::time::timeout(
                settings.run_timeout.saturating_sub(start.elapsed()),
                producer.flush(),
            )
            .await
            .unwrap_or_else(|_| Err(partitionline::Error::protocol("benchmark flush timeout")))
            {
                failure = Some(error);
            }
        }
        if phase == "warmup" {
            warmup_count = sent;
            warmup_elapsed = start.elapsed().as_secs_f64();
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
    let locally_completed = locally_completed_total.saturating_sub(warmup_count);
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
        "{{\"acked\":{acknowledged},\"accepted\":{measured_count},\"locally_completed\":{},\"elapsed_s\":{elapsed:.6},\"acked_rec_s\":{acked_rec_s},\"accepted_rec_s\":{accepted_rec_s:.3},\"delivery_semantics\":\"{}\",\"payload_bytes\":{payload},\"warmup_records\":{warmup_count},\"warmup_elapsed_s\":{warmup_elapsed:.6},\"acknowledged_total\":{acknowledged_total},\"accepted_total\":{next_id},\"effective_settings\":{effective_settings},\"acks\":{acks},\"linger_ms\":{linger_ms},\"compression\":\"{}\",\"idempotent\":{idempotent},\"tls\":{tls_on},\"scram\":{scram_on},\"scram512\":{scram512_on},\"oauthbearer\":{oauth_on},\"record_history\":{},\"integrity_verified\":false,\"performance_claims_valid\":false,\"run_disposition\":\"{disposition}\"}}",
        locally_completed, if acks == 0 { "local_complete" } else { "broker_ack" }, compression.as_str(), journal.is_some()
    );
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}
