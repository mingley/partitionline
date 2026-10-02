//! End-to-end harness tests (KL09-09/10): run representative cells
//! through the real `runtime` binary against real `nb-serve`
//! subprocesses and check each result artifact's fail-closed skeleton.
//!
//! This exercises client connect, the drives, broker reconciliation,
//! and artifact emission. It does not replace
//! `scripts/benchmark-report.py`, which validates full runs.

use std::path::PathBuf;
use std::process::Command;

fn run_cell(cell: &str) -> (serde_json::Value, PathBuf) {
    run_cell_with_args(cell, &[])
}

fn run_cell_with_args(cell: &str, extra: &[&str]) -> (serde_json::Value, PathBuf) {
    let runtime_bin = PathBuf::from(env!("CARGO_BIN_EXE_runtime"));
    let nb_serve_bin = PathBuf::from(env!("CARGO_BIN_EXE_nb-serve"));
    assert!(runtime_bin.is_file());
    assert!(nb_serve_bin.is_file());

    let out_dir = std::env::temp_dir().join(format!(
        "runtime-e2e-{cell}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&out_dir).unwrap();

    let status = Command::new(&runtime_bin)
        .args([
            "--cell",
            cell,
            "--out",
            out_dir.to_str().unwrap(),
            "--repetitions",
            "1",
        ])
        .args(extra)
        .env("NB_SERVE", &nb_serve_bin)
        .status()
        .expect("spawn runtime");
    assert!(
        status.success(),
        "runtime binary failed for {cell}: {status}"
    );

    let result_path = out_dir.join(format!("{cell}-rep0.result.json"));
    assert!(result_path.is_file(), "missing result artifact");
    let text = std::fs::read_to_string(&result_path).unwrap();
    let doc: serde_json::Value = serde_json::from_str(&text).unwrap();
    for section in [
        "schema_version",
        "contract_version",
        "suite_hold",
        "scenario",
        "provenance",
        "execution",
        "outcomes",
        "measurements",
        "integrity",
        "repetition_history",
    ] {
        assert!(doc.get(section).is_some(), "missing section '{section}'");
    }
    assert_eq!(doc["scenario"]["scenario_id"].as_str(), Some(cell));
    assert_eq!(doc["execution"]["repetition_index"], 0);
    assert_eq!(
        doc["scenario"]["cell_disposition"].as_str(),
        Some("executed"),
        "cell must execute cleanly: {}",
        serde_json::to_string_pretty(&doc["execution"]).unwrap()
    );
    // Every sidecar exists and is hashed into provenance.
    let artifacts = doc["provenance"]["artifacts"].as_array().unwrap();
    assert!(!artifacts.is_empty());
    for art in artifacts {
        let path = art["path"].as_str().unwrap();
        assert!(PathBuf::from(path).is_file(), "missing sidecar {path}");
        assert_eq!(art["sha256"].as_str().unwrap().len(), 64);
    }
    (doc, out_dir)
}

#[test]
fn smallest_cell_runs_and_emits_artifact() {
    let (doc, out_dir) = run_cell("nb-send-seq");
    assert_eq!(doc["outcomes"]["offered"], 1000);
    assert_eq!(doc["outcomes"]["acknowledged"], 1000);
    assert_eq!(
        doc["integrity"]["high_watermark_audit"]["total_offset_delta"],
        1000
    );
    assert_eq!(
        doc["integrity"]["high_watermark_audit"]["matches_acknowledged"],
        true
    );
    let _ = std::fs::remove_dir_all(&out_dir);
}

#[test]
fn fetch_bulk_verifies_every_record() {
    let (doc, out_dir) = run_cell("nb-fetch-bulk");
    assert_eq!(doc["scenario"]["profile"], "fetch");
    assert_eq!(doc["outcomes"]["offered"], 20_000);
    assert_eq!(doc["outcomes"]["consumed"], 20_000);
    assert_eq!(doc["outcomes"]["acknowledged"], 0);
    assert_eq!(doc["integrity"]["record_ids"]["expected_count"], 20_000);
    assert_eq!(doc["integrity"]["record_ids"]["verified_count"], 20_000);
    assert_eq!(doc["integrity"]["record_ids"]["missing_ids_count"], 0);
    assert!(doc["execution"]["fetch_rounds"].as_u64().unwrap() >= 1);
    let _ = std::fs::remove_dir_all(&out_dir);
}

#[test]
fn connect_matrix_reports_six_cases() {
    let (doc, out_dir) = run_cell("nb-connect");
    assert_eq!(doc["outcomes"]["offered"], 6);
    assert_eq!(doc["outcomes"]["acknowledged"], 6);
    let cases = doc["execution"]["connect_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 6);
    for case in cases {
        assert_eq!(case["ok"], true);
        assert!(case["first_ack_us"].as_u64().unwrap() > 0);
        assert!(case["open_sockets"].as_u64().unwrap() > 0);
    }
    // Six broker artifacts plus the latency sidecar.
    assert_eq!(doc["provenance"]["artifacts"].as_array().unwrap().len(), 7);
    let _ = std::fs::remove_dir_all(&out_dir);
}

#[cfg(target_os = "linux")]
#[test]
fn connect_matrix_verifies_stalled_tcp_cases() {
    let (doc, out_dir) = run_cell_with_args("nb-connect", &["--connect-stalled-first"]);
    assert_eq!(doc["outcomes"]["offered"], 9);
    assert_eq!(doc["outcomes"]["acknowledged"], 9);
    let cases = doc["execution"]["connect_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 9);
    let stalled = cases
        .iter()
        .filter(|case| case["bootstrap_kind"] == "stalled_tcp")
        .collect::<Vec<_>>();
    assert_eq!(stalled.len(), 3);
    for case in stalled {
        assert_eq!(case["ok"], true);
        assert_eq!(case["connect_timeout_ms"], 200);
        assert!(case["verified_tcp_stall_us"].as_u64().unwrap() >= 45_000);
        assert!(case["first_ack_us"].as_u64().unwrap() > 0);
    }
    assert_eq!(doc["provenance"]["artifacts"].as_array().unwrap().len(), 10);
    let _ = std::fs::remove_dir_all(&out_dir);
}

#[test]
fn capped_paused_cell_prefills_real_client_held_backlog() {
    let (doc, out_dir) = run_cell("nb-fetch-capped-paused");
    assert_eq!(doc["outcomes"]["consumed"], 2_000);
    assert_eq!(doc["execution"]["fetch_rounds"], 2_000);
    assert_eq!(doc["execution"]["fetch_requests"], 1);
    assert_eq!(doc["execution"]["fetched_records"], 120_000);
    assert_eq!(doc["execution"]["paused_backlog_records"], 100_000);
    assert_eq!(doc["execution"]["prefill_buffered_bytes"], 119_999 * 116);
    assert_eq!(
        doc["execution"]["per_partition_delivered"],
        serde_json::json!([{"partition":0,"records":2_000}])
    );
    let _ = std::fs::remove_dir_all(&out_dir);
}
