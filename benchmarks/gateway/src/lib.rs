//! Gateway produce cells versus the isolated rust-rdkafka peer (KL10-02).
//!
//! librdkafka is never a dependency of this crate. The peer is a subprocess
//! of `benchmarks/peers/rust`.

pub mod cells;
pub mod drive;
pub mod json1k;
pub mod peer;
pub mod report;
pub mod rusage;
pub mod schedule;
pub mod settings;

use std::path::{Path, PathBuf};

use crate::cells::{interleave, Cell, ClientKind, Load, CELLS, LOADS};
use crate::report::{not_run_draft, write_result};
use crate::settings::{refuse_ack_modes, refuse_compare, ComparableSettings, TlsMode};

/// Live campaign prerequisites. Absence is a recorded `not_run`, not a crash.
#[derive(Clone, Debug)]
pub struct LiveEnv {
    /// Bootstrap.
    pub bootstrap: String,
    /// Topic.
    pub topic: String,
    /// rust-rdkafka binary.
    pub rust_peer: PathBuf,
    /// TLS mode from the environment.
    pub tls: TlsMode,
}

/// Discover whether a live campaign can run.
pub fn live_env() -> Result<LiveEnv, String> {
    let bootstrap =
        std::env::var("KAFKA_BOOTSTRAP").map_err(|_| "KAFKA_BOOTSTRAP is unset".to_string())?;
    if bootstrap.trim().is_empty() {
        return Err("KAFKA_BOOTSTRAP is empty".into());
    }
    let topic = std::env::var("KAFKA_TOPIC").unwrap_or_else(|_| "pl-gateway".into());
    let rust_peer = peer::rust_peer_binary()?;
    let tls = TlsMode::from_protocol(
        std::env::var("SECURITY_PROTOCOL")
            .unwrap_or_else(|_| "PLAINTEXT".into())
            .as_str(),
        std::env::var("TLS_CLIENT_CERT_PEM").is_ok(),
    );
    Ok(LiveEnv {
        bootstrap,
        topic,
        rust_peer,
        tls,
    })
}

/// One planned A/B step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CampaignStep {
    /// Cell.
    pub cell: Cell,
    /// Load.
    pub load: Load,
    /// Client.
    pub client: ClientKind,
    /// 1-based repetition.
    pub repetition: usize,
    /// Index in the interleaved pair (0 or 1).
    pub pair_index: usize,
}

/// Section 4 campaign: every cell × load × interleaved client pair.
#[must_use]
pub fn campaign_steps(repetitions: usize) -> Vec<CampaignStep> {
    let mut steps = Vec::new();
    for cell in CELLS {
        for load in LOADS {
            for (rep, client) in interleave(repetitions).into_iter().enumerate() {
                steps.push(CampaignStep {
                    cell,
                    load,
                    client,
                    repetition: rep / 2 + 1,
                    pair_index: rep % 2,
                });
            }
        }
    }
    steps
}

/// Run the campaign. Missing broker/peer writes `not_run`; a reachable cluster drives both clients.
pub fn run_campaign(out: &Path, repetitions: usize) -> Result<Vec<PathBuf>, String> {
    match live_env() {
        Ok(env) => run_live_campaign(out, repetitions, &env),
        Err(reason) => write_not_run_campaign(out, repetitions, &reason),
    }
}

fn run_live_campaign(
    out: &Path,
    repetitions: usize,
    env: &LiveEnv,
) -> Result<Vec<PathBuf>, String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    runtime.block_on(run_live_campaign_async(out, repetitions, env))
}

async fn run_live_campaign_async(
    out: &Path,
    repetitions: usize,
    env: &LiveEnv,
) -> Result<Vec<PathBuf>, String> {
    std::fs::create_dir_all(out).map_err(|e| e.to_string())?;
    let count = std::env::var("COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(25_000);
    let warmup = std::env::var("WARMUP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let run_timeout = std::time::Duration::from_millis(
        std::env::var("RUN_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(120_000),
    );
    let mut paths = Vec::new();
    let mut last_settings: Option<(&str, Load, crate::cells::AckMode, ComparableSettings)> = None;
    for (index, step) in campaign_steps(repetitions).into_iter().enumerate() {
        let work = out.join(format!("step-{index:04}"));
        std::fs::create_dir_all(&work).map_err(|e| e.to_string())?;
        let name = format!(
            "{}-{}-{}-rep{}.json",
            step.cell.id,
            step.load.id(),
            step.client.name(),
            step.repetition
        );
        let path = out.join(&name);
        let intended = ComparableSettings::for_cell(step.cell, env.tls);
        let result = match step.client {
            ClientKind::Partitionline => {
                let history = work.join("history.jsonl");
                let outcome = crate::drive::drive_partitionline(&crate::drive::DriveRequest {
                    cell: step.cell,
                    load: step.load,
                    bootstrap: env.bootstrap.clone(),
                    topic: env.topic.clone(),
                    count,
                    warmup,
                    run_timeout,
                    history_path: Some(history.display().to_string()),
                    tls: env.tls,
                })
                .await;
                match outcome {
                    Ok(drive) => {
                        if let Some((id, load, ack, prev)) = last_settings.as_ref() {
                            if *id == step.cell.id && *load == step.load {
                                crate::settings::refuse_ack_modes(*ack, step.cell.ack_mode)
                                    .and_then(|()| {
                                        crate::settings::refuse_compare(prev, &drive.settings)
                                    })
                                    .map_err(|e| e.to_string())?;
                            }
                        }
                        last_settings = Some((
                            step.cell.id,
                            step.load,
                            step.cell.ack_mode,
                            drive.settings.clone(),
                        ));
                        let mut draft = crate::report::not_run_draft(
                            step.cell,
                            step.load,
                            step.client,
                            "executed",
                            step.repetition as u64,
                            repetitions as u64,
                            "A-B-B-A",
                        );
                        draft.disposition = "executed";
                        draft.reason.clear();
                        draft.settings = drive.settings;
                        draft.offered = drive.offered;
                        draft.accepted = drive.accepted;
                        draft.acknowledged = drive.acknowledged;
                        draft.rejected = drive.rejected;
                        draft.timed_out = drive.timed_out;
                        draft.unknown = drive.unknown;
                        draft.verified_ids = drive.verified_ids;
                        draft.elapsed_s = drive.elapsed_s.max(1e-9);
                        draft.latencies_us = drive.latencies_us;
                        draft.rusage = drive.rusage;
                        draft.watermarks = drive.watermarks;
                        draft.watermark_delta = drive.watermark_delta;
                        draft.watermarks_queried = drive.watermarks_queried;
                        draft.artifacts = vec![("history".into(), history)];
                        draft
                    }
                    Err(error) => crate::report::not_run_draft(
                        step.cell,
                        step.load,
                        step.client,
                        error,
                        step.repetition as u64,
                        repetitions as u64,
                        "A-B-B-A",
                    ),
                }
            }
            ClientKind::RustRdkafka => {
                let config = crate::peer::rust_peer_config(
                    step.cell,
                    step.load,
                    &crate::peer::PeerWorkload {
                        bootstrap: &env.bootstrap,
                        topic: &env.topic,
                        count,
                        warmup,
                        tls: env.tls,
                        run_timeout_ms: u64::try_from(run_timeout.as_millis()).unwrap_or(u64::MAX),
                    },
                );
                match crate::peer::emit_peer_config(&env.rust_peer, &config, &work) {
                    Ok(peer_settings) => {
                        if let Err(error) =
                            crate::settings::refuse_compare(&intended, &peer_settings)
                        {
                            return Err(error.to_string());
                        }
                        match crate::peer::run_peer_produce(&env.rust_peer, &config, &work) {
                            Ok(run) => {
                                let timed = run
                                    .raw
                                    .get("timed")
                                    .cloned()
                                    .unwrap_or(serde_json::json!({}));
                                let hw = run
                                    .raw
                                    .get("high_watermarks")
                                    .cloned()
                                    .unwrap_or(serde_json::json!({}));
                                let mut draft = crate::report::not_run_draft(
                                    step.cell,
                                    step.load,
                                    step.client,
                                    if run.exit_code == 0 {
                                        "executed"
                                    } else {
                                        "peer exit"
                                    }
                                    .to_string(),
                                    step.repetition as u64,
                                    repetitions as u64,
                                    "A-B-B-A",
                                );
                                draft.disposition = if run.exit_code == 0 {
                                    "executed"
                                } else {
                                    "failed"
                                };
                                draft.settings = peer_settings;
                                draft.offered =
                                    timed.get("offered").and_then(|v| v.as_u64()).unwrap_or(0);
                                draft.accepted =
                                    timed.get("accepted").and_then(|v| v.as_u64()).unwrap_or(0);
                                draft.acknowledged = timed
                                    .get("acknowledged")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0);
                                draft.rejected =
                                    timed.get("rejected").and_then(|v| v.as_u64()).unwrap_or(0);
                                draft.timed_out =
                                    timed.get("timed_out").and_then(|v| v.as_u64()).unwrap_or(0);
                                draft.unknown =
                                    timed.get("unknown").and_then(|v| v.as_u64()).unwrap_or(0);
                                draft.elapsed_s = timed
                                    .get("elapsed_s")
                                    .and_then(|v| v.as_f64())
                                    .unwrap_or(1e-9)
                                    .max(1e-9);
                                draft.rusage = run.rusage;
                                draft.watermark_delta = hw
                                    .get("total_offset_delta")
                                    .and_then(|v| v.as_i64())
                                    .unwrap_or(0);
                                draft.watermarks_queried =
                                    hw.get("queried").and_then(|v| v.as_bool()).unwrap_or(false);
                                draft.watermarks = hw
                                    .get("partitions")
                                    .cloned()
                                    .unwrap_or(serde_json::json!([]))
                                    .as_array()
                                    .cloned()
                                    .unwrap_or_default();
                                last_settings = Some((
                                    step.cell.id,
                                    step.load,
                                    step.cell.ack_mode,
                                    draft.settings.clone(),
                                ));
                                draft
                            }
                            Err(error) => crate::report::not_run_draft(
                                step.cell,
                                step.load,
                                step.client,
                                error,
                                step.repetition as u64,
                                repetitions as u64,
                                "A-B-B-A",
                            ),
                        }
                    }
                    Err(error) => crate::report::not_run_draft(
                        step.cell,
                        step.load,
                        step.client,
                        error,
                        step.repetition as u64,
                        repetitions as u64,
                        "A-B-B-A",
                    ),
                }
            }
        };
        crate::report::write_result(&path, &result)?;
        paths.push(path);
    }
    Ok(paths)
}

/// Write `not_run` results for every planned step when live prerequisites are missing.
pub fn write_not_run_campaign(
    out: &Path,
    repetitions: usize,
    reason: &str,
) -> Result<Vec<PathBuf>, String> {
    std::fs::create_dir_all(out).map_err(|e| e.to_string())?;
    let mut paths = Vec::new();
    for (index, step) in campaign_steps(repetitions).into_iter().enumerate() {
        let name = format!(
            "{}-{}-{}-rep{}.json",
            step.cell.id,
            step.load.id(),
            step.client.name(),
            step.repetition
        );
        let path = out.join(name);
        let order = format!(
            "rep{} {} then {}",
            step.repetition,
            if step.pair_index == 0 {
                step.client.name()
            } else {
                "paired"
            },
            "interleaved A-B-B-A"
        );
        let draft = not_run_draft(
            step.cell,
            step.load,
            step.client,
            format!("{reason} (step {index})"),
            step.repetition as u64,
            repetitions as u64,
            order,
        );
        write_result(&path, &draft)?;
        paths.push(path);
    }
    Ok(paths)
}

/// Compare two effective-config JSON objects, including acknowledgment mode.
pub fn compare_effective(
    left: &serde_json::Value,
    right: &serde_json::Value,
    left_ack: crate::cells::AckMode,
    right_ack: crate::cells::AckMode,
) -> Result<(ComparableSettings, ComparableSettings), String> {
    refuse_ack_modes(left_ack, right_ack).map_err(|e| e.to_string())?;
    let left_s = ComparableSettings::from_effective(left)?;
    let right_s = ComparableSettings::from_effective(right)?;
    refuse_compare(&left_s, &right_s).map_err(|e| e.to_string())?;
    Ok((left_s, right_s))
}

/// Partitionline effective JSON for a cell (no broker).
#[must_use]
pub fn partitionline_emit_config(cell: Cell, tls: TlsMode) -> serde_json::Value {
    let settings = ComparableSettings::for_cell(cell, tls);
    serde_json::json!({
        "client": "partitionline",
        "cell": cell.id,
        "acknowledgment_mode": cell.ack_mode.label(),
        "acks": settings.acks,
        "linger_ms": settings.linger_ms,
        "batch_size_bytes": settings.batch_size_bytes,
        "max_in_flight": settings.max_in_flight,
        "codec": settings.codec,
        "compression": settings.codec,
        "level": settings.level,
        "security_protocol": settings.tls.security_protocol(),
        "tls_client_cert": matches!(settings.tls, TlsMode::Mtls),
        "idempotence": false,
        "payload_bytes": json1k::VALUE_BYTES,
        "payload_mode": "json1k",
        "header_mode": "gw8",
        "partitions": cell.partitions,
        "record_seed": json1k::SEED,
    })
}
