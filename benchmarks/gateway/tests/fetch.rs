use gateway::cells::{
    find_fetch, interleave, AckMode, ClientKind, Load, FETCH_CELLS, FETCH_MAX_BYTES,
    FETCH_MAX_WAIT_MS, FETCH_MIN_BYTES, FETCH_PARTITION_BYTES,
};
use gateway::fetch::{compare_fetch_effective, partitionline_fetch_emit_config};
use gateway::fetch::{fetch_campaign_steps, write_not_run_fetch_campaign};
use gateway::report::{not_run_fetch_draft, verified_throughput, write_result};
use gateway::settings::{refuse_fetch_compare, FetchSettings, TlsMode};
use serde_json::json;
use std::path::PathBuf;
use std::process::Command;

#[test]
fn fetch_cell_matches_gateway_profile() {
    let cell = find_fetch("lb-gateway-fetch-gzip").unwrap();
    assert_eq!(FETCH_CELLS.len(), 1);
    assert_eq!(cell.id, "lb-gateway-fetch-gzip");
    assert_eq!(cell.codec, "gzip");
    assert_eq!(cell.partitions, 8);
    assert_eq!(cell.ack_mode, AckMode::ManualAssignment);
    assert_eq!(FETCH_MIN_BYTES, 1);
    assert_eq!(FETCH_MAX_WAIT_MS, 500);
    assert_eq!(FETCH_MAX_BYTES, 52_428_800);
    assert_eq!(FETCH_PARTITION_BYTES, 1_048_576);
    assert_eq!(Load::SeededBacklog.id(), "backlog");
}

#[test]
fn fetch_interleave_is_ten_abba_steps() {
    assert_eq!(
        interleave(5),
        [
            ClientKind::Partitionline,
            ClientKind::RustRdkafka,
            ClientKind::RustRdkafka,
            ClientKind::Partitionline,
            ClientKind::Partitionline,
            ClientKind::RustRdkafka,
            ClientKind::RustRdkafka,
            ClientKind::Partitionline,
            ClientKind::Partitionline,
            ClientKind::RustRdkafka,
        ]
    );
    let steps = fetch_campaign_steps(5);
    assert_eq!(steps.len(), 10);
    assert!(steps
        .iter()
        .all(|step| step.cell.id == "lb-gateway-fetch-gzip"));
}

#[test]
fn fetch_settings_refuse_each_gated_field() {
    let left = FetchSettings::for_cell(TlsMode::Plain);
    refuse_fetch_compare(&left, &left).unwrap();
    let mut min_bytes = left.clone();
    min_bytes.min_bytes = 64;
    let mut wait = left.clone();
    wait.max_wait_ms = 50;
    let mut fetch_max = left.clone();
    fetch_max.fetch_max_bytes = 1_000;
    let mut part = left.clone();
    part.max_partition_fetch_bytes = 2_000;
    let mut tls = left.clone();
    tls.tls = TlsMode::Mtls;
    let mut isolation = left.clone();
    isolation.isolation = "read_committed".into();
    for (field, right) in [
        ("min_bytes", min_bytes),
        ("max_wait_ms", wait),
        ("fetch_max_bytes", fetch_max),
        ("max_partition_fetch_bytes", part),
        ("tls", tls),
        ("isolation", isolation),
    ] {
        assert_eq!(
            refuse_fetch_compare(&left, &right).unwrap_err().field,
            field
        );
    }
}

#[test]
fn fetch_emit_config_round_trips_peer_keys() {
    let cell = find_fetch("lb-gateway-fetch-gzip").unwrap();
    let emitted = partitionline_fetch_emit_config(cell, TlsMode::Plain);
    let parsed = FetchSettings::from_effective(&emitted).unwrap();
    assert_eq!(parsed, FetchSettings::for_cell(TlsMode::Plain));
    let peer = json!({
        "fetch.min.bytes": "1",
        "fetch.wait.max.ms": "500",
        "fetch.max.bytes": "52428800",
        "max.partition.fetch.bytes": "1048576",
        "isolation.level": "read_uncommitted",
        "security.protocol": "plaintext"
    });
    compare_fetch_effective(&emitted, &peer).unwrap();
}

#[test]
fn fetch_throughput_uses_verified_counts() {
    assert_eq!(verified_throughput(22_000, 1.0), 22_000.0);
    assert_eq!(verified_throughput(0, 2.0), 0.0);
}

#[test]
fn fetch_not_run_validates_with_benchmark_report() {
    let dir = tempfile("gateway-fetch-report");
    let cell = find_fetch("lb-gateway-fetch-gzip").unwrap();
    let path = dir.join("not-run.json");
    let draft = not_run_fetch_draft(
        cell,
        ClientKind::Partitionline,
        "unit-test fixture: no broker",
        1,
        5,
        "A-B-B-A unit test",
    );
    write_result(&path, &draft).unwrap();
    let status = Command::new("python3")
        .args(["scripts/benchmark-report.py", path.to_str().expect("utf8")])
        .current_dir(workspace_root())
        .status()
        .expect("python3");
    assert!(status.success(), "benchmark-report.py failed");
}

#[test]
fn fetch_campaign_writes_ten_not_run_files() {
    let dir = tempfile("gateway-fetch-campaign");
    let paths = write_not_run_fetch_campaign(&dir, 5, "unit-test live blocker").unwrap();
    assert_eq!(paths.len(), 10);
    for path in &paths {
        let status = Command::new("python3")
            .args(["scripts/benchmark-report.py", path.to_str().expect("utf8")])
            .current_dir(workspace_root())
            .status()
            .expect("python3");
        assert!(
            status.success(),
            "benchmark-report.py rejected {}",
            path.display()
        );
    }
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn tempfile(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "{prefix}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
