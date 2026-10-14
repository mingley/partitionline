//! Gateway fetch campaign: one seeded gzip backlog, interleaved A/B consume.

use std::path::{Path, PathBuf};

use serde_json::json;

use crate::cells::{interleave, Cell, ClientKind, Load, FETCH_CELLS, FETCH_GZIP, GZIP};
use crate::report::{not_run_fetch_draft, write_result, ResultDraft};
use crate::settings::{refuse_fetch_compare, FetchSettings, TlsMode};
use crate::{live_env, LiveEnv};

/// One planned fetch A/B step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchStep {
    /// Cell.
    pub cell: Cell,
    /// Client.
    pub client: ClientKind,
    /// 1-based repetition.
    pub repetition: usize,
    /// Index in the interleaved pair (0 or 1).
    pub pair_index: usize,
}

/// Section 4 fetch campaign: one cell × interleaved client pair.
#[must_use]
pub fn fetch_campaign_steps(repetitions: usize) -> Vec<FetchStep> {
    let mut steps = Vec::new();
    for cell in FETCH_CELLS {
        for (rep, client) in interleave(repetitions).into_iter().enumerate() {
            steps.push(FetchStep {
                cell,
                client,
                repetition: rep / 2 + 1,
                pair_index: rep % 2,
            });
        }
    }
    steps
}

/// Run the fetch campaign. Missing broker/peer writes `not_run`.
pub fn run_fetch_campaign(out: &Path, repetitions: usize) -> Result<Vec<PathBuf>, String> {
    match live_env() {
        Ok(env) => run_live_fetch_campaign(out, repetitions, &env),
        Err(reason) => write_not_run_fetch_campaign(out, repetitions, &reason),
    }
}

fn run_live_fetch_campaign(
    out: &Path,
    repetitions: usize,
    env: &LiveEnv,
) -> Result<Vec<PathBuf>, String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    runtime.block_on(run_live_fetch_async(out, repetitions, env))
}

async fn run_live_fetch_async(
    out: &Path,
    repetitions: usize,
    env: &LiveEnv,
) -> Result<Vec<PathBuf>, String> {
    std::fs::create_dir_all(out).map_err(|e| e.to_string())?;
    let count = std::env::var("COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(25_000);
    let run_timeout = std::time::Duration::from_millis(
        std::env::var("RUN_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(120_000),
    );
    let topic = std::env::var("KAFKA_FETCH_TOPIC").unwrap_or_else(|_| "pl-gateway-fetch".into());
    let seed_dir = out.join("backlog");
    std::fs::create_dir_all(&seed_dir).map_err(|e| e.to_string())?;
    let backlog = match seed_backlog(env, &topic, count, run_timeout, &seed_dir).await {
        Ok(backlog) => backlog,
        Err(reason) => {
            return write_not_run_fetch_campaign(out, repetitions, &reason);
        }
    };
    let intended = FetchSettings::for_cell(env.tls);
    let mut last_settings: Option<FetchSettings> = None;
    let mut paths = Vec::new();
    for (index, step) in fetch_campaign_steps(repetitions).into_iter().enumerate() {
        let work = out.join(format!("step-{index:04}"));
        std::fs::create_dir_all(&work).map_err(|e| e.to_string())?;
        let name = format!(
            "{}-{}-{}-rep{}.json",
            step.cell.id,
            Load::SeededBacklog.id(),
            step.client.name(),
            step.repetition
        );
        let path = out.join(&name);
        let draft = match step.client {
            ClientKind::Partitionline => {
                match crate::fdrive::drive_partitionline_fetch(&crate::fdrive::FetchRequest {
                    cell: step.cell,
                    bootstrap: env.bootstrap.clone(),
                    topic: topic.clone(),
                    count: backlog.count,
                    assignment_offsets: &backlog.start_offsets,
                    expected_hashes: &backlog.hashes,
                    run_timeout,
                    tls: env.tls,
                })
                .await
                {
                    Ok(drive) => {
                        if let Some(prev) = last_settings.as_ref() {
                            refuse_fetch_compare(prev, &drive.settings)
                                .map_err(|e| e.to_string())?;
                        }
                        last_settings = Some(drive.settings.clone());
                        fetch_draft(FetchDraftInput {
                            step,
                            repetitions,
                            reason: "executed",
                            settings: drive.settings,
                            offered: drive.offered,
                            verified_ids: drive.verified_ids,
                            rejected: drive.rejected,
                            timed_out: drive.timed_out,
                            elapsed_s: drive.elapsed_s,
                            rusage: drive.rusage,
                            backlog: &backlog,
                            artifacts: vec![(
                                "history".into(),
                                seed_dir.join("peer.attempts.jsonl"),
                            )],
                        })
                    }
                    Err(error) => not_run_fetch_draft(
                        step.cell,
                        step.client,
                        error,
                        step.repetition as u64,
                        repetitions as u64,
                        "A-B-B-A",
                    ),
                }
            }
            ClientKind::RustRdkafka => {
                let config = crate::peer::rust_peer_fetch_config(
                    step.cell,
                    &crate::peer::FetchWorkload {
                        bootstrap: &env.bootstrap,
                        topic: &topic,
                        count: backlog.count,
                        assignment_offsets: &backlog.start_offsets,
                        tls: env.tls,
                        run_timeout_ms: u64::try_from(run_timeout.as_millis()).unwrap_or(u64::MAX),
                    },
                );
                match crate::peer::emit_peer_consumer_config(&env.rust_peer, &config, &work) {
                    Ok(peer_settings) => {
                        refuse_fetch_compare(&intended, &peer_settings)
                            .map_err(|e| e.to_string())?;
                        if let Some(prev) = last_settings.as_ref() {
                            refuse_fetch_compare(prev, &peer_settings)
                                .map_err(|e| e.to_string())?;
                        }
                        match crate::peer::run_peer_consume(&env.rust_peer, &config, &work) {
                            Ok(run) => {
                                let timed = run.raw.get("timed").cloned().unwrap_or(json!({}));
                                let verification =
                                    run.raw.get("verification").cloned().unwrap_or(json!({}));
                                let history = work.join("peer.attempts.jsonl");
                                let verified = crate::peer::count_verified_history(&history);
                                let mut draft = fetch_draft(FetchDraftInput {
                                    step,
                                    repetitions,
                                    reason: if run.exit_code == 0 {
                                        "executed"
                                    } else {
                                        "peer exit"
                                    },
                                    settings: peer_settings.clone(),
                                    offered: timed
                                        .get("offered")
                                        .and_then(|v| v.as_u64())
                                        .unwrap_or(0),
                                    verified_ids: verified.max(
                                        verification
                                            .get("verified_ids")
                                            .and_then(|v| v.as_u64())
                                            .unwrap_or(0),
                                    ),
                                    rejected: timed
                                        .get("rejected")
                                        .and_then(|v| v.as_u64())
                                        .unwrap_or(0),
                                    timed_out: timed
                                        .get("timed_out")
                                        .and_then(|v| v.as_u64())
                                        .unwrap_or(0),
                                    elapsed_s: timed
                                        .get("elapsed_s")
                                        .and_then(|v| v.as_f64())
                                        .unwrap_or(1e-9)
                                        .max(1e-9),
                                    rusage: run.rusage,
                                    backlog: &backlog,
                                    artifacts: vec![
                                        ("history".into(), history),
                                        ("raw".into(), work.join("peer.raw.json")),
                                        (
                                            "seed-history".into(),
                                            seed_dir.join("peer.attempts.jsonl"),
                                        ),
                                    ],
                                });
                                if run.exit_code != 0 {
                                    draft.disposition = "failed";
                                }
                                last_settings = Some(peer_settings);
                                draft
                            }
                            Err(error) => not_run_fetch_draft(
                                step.cell,
                                step.client,
                                error,
                                step.repetition as u64,
                                repetitions as u64,
                                "A-B-B-A",
                            ),
                        }
                    }
                    Err(error) => not_run_fetch_draft(
                        step.cell,
                        step.client,
                        error,
                        step.repetition as u64,
                        repetitions as u64,
                        "A-B-B-A",
                    ),
                }
            }
        };
        write_result(&path, &draft)?;
        paths.push(path);
    }
    Ok(paths)
}

struct Backlog {
    count: u64,
    start_offsets: Vec<i64>,
    hashes: std::collections::BTreeMap<u64, String>,
    watermarks: Vec<serde_json::Value>,
    watermark_delta: i64,
}

async fn seed_backlog(
    env: &LiveEnv,
    topic: &str,
    count: u64,
    run_timeout: std::time::Duration,
    work: &Path,
) -> Result<Backlog, String> {
    ensure_topic(&env.bootstrap, topic, FETCH_GZIP.partitions).await?;
    let start = watermarks(&env.bootstrap, topic, FETCH_GZIP.partitions).await?;
    let config = crate::peer::rust_peer_config(
        GZIP,
        Load::Unpaced,
        &crate::peer::PeerWorkload {
            bootstrap: &env.bootstrap,
            topic,
            count,
            warmup: 0,
            tls: env.tls,
            run_timeout_ms: u64::try_from(run_timeout.as_millis()).unwrap_or(u64::MAX),
        },
    );
    crate::peer::emit_peer_config(&env.rust_peer, &config, work)
        .map_err(|e| format!("seed emit-config: {e}"))?;
    let run = crate::peer::run_peer_produce(&env.rust_peer, &config, work)
        .map_err(|e| format!("seed produce: {e}"))?;
    if run.exit_code != 0 {
        return Err(format!("seed produce exited {0}", run.exit_code));
    }
    let history = work.join("peer.attempts.jsonl");
    let hashes = crate::peer::load_acked_hashes(&history)?;
    if hashes.len() as u64 != count {
        return Err(format!(
            "seed history has {} acked ids; expected {count}",
            hashes.len()
        ));
    }
    let end = watermarks(&env.bootstrap, topic, FETCH_GZIP.partitions).await?;
    if start.len() != end.len() {
        return Err("watermark partition count changed during seed".into());
    }
    let rows: Vec<_> = start
        .iter()
        .zip(&end)
        .enumerate()
        .map(|(p, (a, b))| {
            json!({
                "partition": p,
                "start_offset": a,
                "end_offset": b,
                "offset_delta": (*b - *a).max(0)
            })
        })
        .collect();
    let delta: i64 = start.iter().zip(&end).map(|(a, b)| (*b - *a).max(0)).sum();
    if delta != count as i64 {
        return Err(format!(
            "seed high-watermark delta {delta} does not match COUNT {count}"
        ));
    }
    let manifest = json!({
        "topic": topic,
        "count": count,
        "start_offsets": start,
        "end_offsets": end,
        "codec": "gzip",
        "seed": crate::json1k::SEED,
        "writer": "rust-rdkafka",
        "reused": true
    });
    std::fs::write(
        work.join("backlog.json"),
        serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    Ok(Backlog {
        count,
        start_offsets: start,
        hashes,
        watermarks: rows,
        watermark_delta: delta,
    })
}

async fn ensure_topic(bootstrap: &str, topic: &str, partitions: i32) -> Result<(), String> {
    use partitionline::{Admin, NewTopic};
    let mut admin = Admin::connect(bootstrap)
        .await
        .map_err(|e| format!("admin: {e}"))?;
    let created = admin
        .create_topics(&[NewTopic::new(topic, partitions, 1)], 30_000, false)
        .await
        .map_err(|e| format!("create_topics: {e}"))?;
    for result in created {
        if result.error_code != 0 && result.error_code != 36 {
            return Err(format!(
                "create topic {} failed: error_code {}",
                result.name, result.error_code
            ));
        }
    }
    let _ = admin.close().await;
    Ok(())
}

async fn watermarks(bootstrap: &str, topic: &str, partitions: i32) -> Result<Vec<i64>, String> {
    use partitionline::{Admin, TopicPartition, LATEST_TIMESTAMP};
    let mut last = None;
    for _ in 0..20 {
        let mut admin = Admin::connect(bootstrap)
            .await
            .map_err(|e| format!("admin: {e}"))?;
        let queries: Vec<_> = (0..partitions)
            .map(|partition| (TopicPartition::new(topic, partition), LATEST_TIMESTAMP))
            .collect();
        match admin.list_offsets(queries).await {
            Ok(listed) => {
                let mut highs = vec![-1i64; partitions as usize];
                for (tp, offset) in listed {
                    if let Ok(idx) = usize::try_from(tp.partition) {
                        if let Some(slot) = highs.get_mut(idx) {
                            *slot = offset.offset.max(0);
                        }
                    }
                }
                let _ = admin.close().await;
                if highs.iter().all(|h| *h >= 0) {
                    return Ok(highs);
                }
                last = Some("topic partitions not ready".to_string());
            }
            Err(error) => last = Some(format!("list_offsets: {error}")),
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    Err(last.unwrap_or_else(|| "list_offsets failed".into()))
}

struct FetchDraftInput<'a> {
    step: FetchStep,
    repetitions: usize,
    reason: &'a str,
    settings: FetchSettings,
    offered: u64,
    verified_ids: u64,
    rejected: u64,
    timed_out: u64,
    elapsed_s: f64,
    rusage: crate::rusage::ProcessRusage,
    backlog: &'a Backlog,
    artifacts: Vec<(String, PathBuf)>,
}

fn fetch_draft(input: FetchDraftInput<'_>) -> ResultDraft {
    let mut draft = not_run_fetch_draft(
        input.step.cell,
        input.step.client,
        if input.reason == "executed" {
            String::new()
        } else {
            input.reason.to_string()
        },
        input.step.repetition as u64,
        input.repetitions as u64,
        "A-B-B-A",
    );
    draft.disposition = if input.reason == "executed" {
        "executed"
    } else {
        "failed"
    };
    draft.settings =
        crate::settings::ComparableSettings::for_cell(input.step.cell, input.settings.tls);
    draft.fetch = Some(input.settings);
    draft.offered = input.offered;
    draft.accepted = input.offered;
    draft.acknowledged = 0;
    draft.verified_ids = input.verified_ids;
    draft.rejected = input.rejected;
    draft.timed_out = input.timed_out;
    draft.unknown = 0;
    draft.elapsed_s = input.elapsed_s.max(1e-9);
    draft.rusage = input.rusage;
    draft.watermarks = input.backlog.watermarks.clone();
    draft.watermark_delta = input.backlog.watermark_delta;
    draft.watermarks_queried = true;
    draft.artifacts = input.artifacts;
    draft
}

/// Write `not_run` results for every planned fetch step.
pub fn write_not_run_fetch_campaign(
    out: &Path,
    repetitions: usize,
    reason: &str,
) -> Result<Vec<PathBuf>, String> {
    std::fs::create_dir_all(out).map_err(|e| e.to_string())?;
    let mut paths = Vec::new();
    for (index, step) in fetch_campaign_steps(repetitions).into_iter().enumerate() {
        let name = format!(
            "{}-{}-{}-rep{}.json",
            step.cell.id,
            Load::SeededBacklog.id(),
            step.client.name(),
            step.repetition
        );
        let path = out.join(name);
        let draft = not_run_fetch_draft(
            step.cell,
            step.client,
            format!("{reason} (step {index})"),
            step.repetition as u64,
            repetitions as u64,
            "A-B-B-A",
        );
        write_result(&path, &draft)?;
        paths.push(path);
    }
    Ok(paths)
}

/// Partitionline fetch effective JSON (no broker).
#[must_use]
pub fn partitionline_fetch_emit_config(cell: Cell, tls: TlsMode) -> serde_json::Value {
    let settings = FetchSettings::for_cell(tls);
    json!({
        "client": "partitionline",
        "cell": cell.id,
        "acknowledgment_mode": cell.ack_mode.label(),
        "fetch_min_bytes": settings.min_bytes,
        "max_wait_ms": settings.max_wait_ms,
        "fetch_max_bytes": settings.fetch_max_bytes,
        "max_partition_fetch_bytes": settings.max_partition_fetch_bytes,
        "isolation": settings.isolation,
        "security_protocol": settings.tls.security_protocol(),
        "tls_client_cert": matches!(settings.tls, TlsMode::Mtls),
        "payload_bytes": crate::json1k::VALUE_BYTES,
        "payload_mode": "json1k",
        "header_mode": "gw8",
        "partitions": cell.partitions,
        "record_seed": crate::json1k::SEED,
        "assignment": "manual",
        "codec": cell.codec,
    })
}

/// Compare two fetch effective-config objects.
pub fn compare_fetch_effective(
    left: &serde_json::Value,
    right: &serde_json::Value,
) -> Result<(FetchSettings, FetchSettings), String> {
    let left_s = FetchSettings::from_effective(left)?;
    let right_s = FetchSettings::from_effective(right)?;
    refuse_fetch_compare(&left_s, &right_s).map_err(|e| e.to_string())?;
    Ok((left_s, right_s))
}
