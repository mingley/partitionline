//! Host-probe and measurement smoke tests (KL09-09): real machine
//! facts come back sane, CPU/RSS samplers return non-degenerate
//! values, and the broker-artifact parser fails closed.

use std::time::Duration;

use runtime::artifact::parse_broker_artifact;
use runtime::host::probe;
use runtime::measure::{cpu_now, peak_rss_bytes, rss_now, RssSampler};

#[test]
fn host_probe_returns_machine_facts() {
    let host = probe().expect("host probe must succeed");
    assert!(!host.hostname.is_empty());
    assert!(!host.os.is_empty());
    assert!(!host.arch.is_empty());
    assert!(!host.cpu_model.is_empty() || host.cpu_model == "unknown");
    assert!(host.logical_cores >= 1);
    assert!(host.physical_cores >= 1);
    assert!(host.physical_cores <= host.logical_cores);
    assert!(host.memory_total_bytes > 0);
}

#[test]
fn cpu_and_rss_observe_something() {
    // Burn a little CPU so the delta is nonzero.
    let before = cpu_now();
    let mut acc = 0u64;
    for i in 0..1_000_000u64 {
        acc = acc.wrapping_add(i ^ (i >> 7));
    }
    std::hint::black_box(acc);
    let after = cpu_now();
    assert!(after.total_us() >= before.total_us());
    assert!(rss_now() > 0);
    assert!(peak_rss_bytes() >= rss_now());
}

#[test]
fn rss_sampler_summarizes() {
    let sampler = RssSampler::start(Duration::from_millis(5), 100);
    std::thread::sleep(Duration::from_millis(30));
    let (peak, mean) = sampler.stop();
    assert!(peak > 0);
    assert!(mean > 0);
    assert!(peak >= mean);
}

#[test]
fn broker_artifact_parses_and_rejects_gaps() {
    let dir = std::env::temp_dir().join(format!("runtime-artifact-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let good = dir.join("good.broker.json");
    std::fs::write(
        &good,
        r#"{
  "accepted_records": 1000,
  "accepted_wire_bytes": 128000,
  "produce_requests": 10,
  "metadata_requests": 4,
  "injected_requests": 2,
  "injected_errors": 12,
  "fetch_requests": 7,
  "fetched_records": 700,
  "validation_failures": {"framing": 0, "crc": 1, "count": 0, "sequence": 0, "transactional": 0},
  "end_offsets": {"nb-seq/0": 500, "nb-seq/1": 500}
}"#,
    )
    .unwrap();
    let counts = parse_broker_artifact(&good).expect("valid artifact parses");
    assert_eq!(counts.accepted_records, 1000);
    assert_eq!(counts.produce_requests, 10);
    assert_eq!(counts.validation_failures, 1);
    assert_eq!(counts.end_offsets.len(), 2);
    assert_eq!(
        counts.end_offsets.iter().map(|(_, _, o)| o).sum::<i64>(),
        1000
    );

    let bad = dir.join("bad.broker.json");
    std::fs::write(&bad, r#"{"accepted_records": 5}"#).unwrap();
    assert!(parse_broker_artifact(&bad).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn broker_counts_merge_for_multi_broker_cells() {
    use runtime::artifact::BrokerCounts;
    let mut a = BrokerCounts {
        accepted_records: 3,
        produce_requests: 3,
        metadata_requests: 10,
        injected_requests: 1,
        injected_errors: 6,
        end_offsets: vec![("t".to_owned(), 0, 3)],
        ..BrokerCounts::default()
    };
    let b = BrokerCounts {
        accepted_records: 3,
        produce_requests: 3,
        metadata_requests: 10,
        injected_requests: 0,
        injected_errors: 0,
        end_offsets: vec![("t".to_owned(), 0, 3), ("t".to_owned(), 1, 1)],
        ..BrokerCounts::default()
    };
    a.merge(&b);
    assert_eq!(a.accepted_records, 6);
    assert_eq!(a.produce_requests, 6);
    assert_eq!(a.metadata_requests, 20);
    assert_eq!(a.injected_requests, 1);
    assert_eq!(a.injected_errors, 6);
    assert_eq!(
        a.end_offsets,
        vec![("t".to_owned(), 0, 6), ("t".to_owned(), 1, 1)]
    );
}
