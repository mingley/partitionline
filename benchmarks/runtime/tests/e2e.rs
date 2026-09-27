//! End-to-end harness test (KL09-09): run the smallest producer
//! cell (`nb-send-seq`, 1000 records) through the real `runtime`
//! binary against a real `nb-serve` subprocess and check the result
//! artifact's fail-closed skeleton.
//!
//! This exercises producer connect, the sequential drive, broker
//! reconciliation, and artifact emission. It does not replace
//! `scripts/benchmark-report.py`, which validates full runs.

use std::path::PathBuf;
use std::process::Command;

#[test]
fn smallest_cell_runs_and_emits_artifact() {
    let runtime_bin = PathBuf::from(env!("CARGO_BIN_EXE_runtime"));
    let nb_serve_bin = PathBuf::from(env!("CARGO_BIN_EXE_nb-serve"));
    assert!(runtime_bin.is_file());
    assert!(nb_serve_bin.is_file());

    let out_dir = std::env::temp_dir().join(format!(
        "runtime-e2e-{}-{}",
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
            "nb-send-seq",
            "--out",
            out_dir.to_str().unwrap(),
            "--repetitions",
            "1",
        ])
        .env("NB_SERVE", &nb_serve_bin)
        .status()
        .expect("spawn runtime");
    assert!(
        status.success(),
        "runtime binary failed for nb-send-seq: {status}"
    );

    let result_path = out_dir.join("nb-send-seq-rep0.result.json");
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
    assert_eq!(doc["scenario"]["scenario_id"].as_str(), Some("nb-send-seq"));
    assert_eq!(doc["execution"]["repetition_index"], 0);
    assert_eq!(
        doc["scenario"]["cell_disposition"].as_str(),
        Some("executed"),
        "cell must execute cleanly: {}",
        serde_json::to_string_pretty(&doc["execution"]).unwrap()
    );
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
    // Broker + latency sidecars exist and are hashed into provenance.
    let artifacts = doc["provenance"]["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), 2);
    for art in artifacts {
        let path = art["path"].as_str().unwrap();
        assert!(PathBuf::from(path).is_file(), "missing sidecar {path}");
        assert_eq!(art["sha256"].as_str().unwrap().len(), 64);
    }

    let _ = std::fs::remove_dir_all(&out_dir);
}
