use gateway::cells::{find, ClientKind, Load};
use gateway::report::{acknowledged_throughput, not_run_draft, write_result};
use gateway::write_not_run_campaign;
use std::path::PathBuf;
use std::process::Command;

#[test]
fn throughput_never_uses_enqueue_counts() {
    assert_eq!(acknowledged_throughput(8000, 2.0), 4000.0);
    assert_eq!(acknowledged_throughput(0, 2.0), 0.0);
    // 10000 offered / 2s would be 5000; that rate is not this function.
    assert_ne!(acknowledged_throughput(8000, 2.0), 10_000.0 / 2.0);
}

#[test]
fn not_run_result_validates_with_benchmark_report() {
    let dir = tempfile("gateway-report");
    let cell = find("lb-gateway-produce-gzip").unwrap();
    let path = dir.join("not-run.json");
    let draft = not_run_draft(
        cell,
        Load::OpenLoop25k,
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
    assert!(
        status.success(),
        "benchmark-report.py failed on {}",
        path.display()
    );
}

#[test]
fn campaign_writes_thirty_not_run_files() {
    let dir = tempfile("gateway-campaign");
    let paths = write_not_run_campaign(&dir, 5, "unit-test live blocker").unwrap();
    assert_eq!(paths.len(), 60);
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

#[test]
fn gateway_manifest_does_not_depend_on_rdkafka() {
    let manifest =
        std::fs::read_to_string(workspace_root().join("benchmarks/gateway/Cargo.toml")).unwrap();
    let deps = manifest
        .split("[dev-dependencies]")
        .next()
        .and_then(|text| text.split("[dependencies]").nth(1))
        .unwrap_or("");
    assert!(!deps.contains("rdkafka"));
    assert!(!deps.contains("rdkafka-sys"));
    let lock =
        std::fs::read_to_string(workspace_root().join("benchmarks/gateway/Cargo.lock")).unwrap();
    assert!(
        !lock.contains("name = \"rdkafka\"") && !lock.contains("name = \"rdkafka-sys\""),
        "gateway lockfile must not include librdkafka packages"
    );
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
