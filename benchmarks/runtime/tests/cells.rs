//! Cell-definition contract tests (KL09-09): the eight section-4
//! producer cells exist exactly as defined, generation is
//! deterministic, partitions are explicit round-robin, and record IDs
//! are embedded 1-based in the value prefix.

use runtime::cells::{generate, producer_cells, DriveMode};

#[test]
fn eight_cells_exact_ids_and_counts() {
    let cells = producer_cells();
    let ids: Vec<&str> = cells.iter().map(|c| c.id).collect();
    assert_eq!(
        ids,
        vec![
            "nb-produce-bulk",
            "nb-produce-idem",
            "nb-produce-mixed-topics",
            "nb-produce-headers",
            "nb-produce-128p",
            "nb-produce-flush-heavy",
            "nb-send-seq",
            "nb-produce-idle-rss",
        ]
    );
    let totals: Vec<usize> = cells.iter().map(|c| c.total_records()).collect();
    assert_eq!(
        totals,
        vec![20_000, 20_000, 12_000, 5_000, 256_000, 2_000, 1_000, 0]
    );
}

#[test]
fn cell_modes_and_idempotence() {
    let cells = producer_cells();
    let by_id = |id: &str| cells.iter().find(|c| c.id == id).unwrap();
    assert_eq!(by_id("nb-send-seq").mode, DriveMode::Sequential);
    assert_eq!(by_id("nb-produce-flush-heavy").mode, DriveMode::FlushHeavy);
    assert_eq!(by_id("nb-produce-idle-rss").mode, DriveMode::Idle);
    assert_eq!(by_id("nb-produce-bulk").mode, DriveMode::Pipelined);
    assert!(by_id("nb-produce-idem").idempotent);
    assert_eq!(by_id("nb-produce-idem").acks, -1);
    assert!(!by_id("nb-produce-bulk").idempotent);
    assert_eq!(by_id("nb-produce-headers").headers_each, 3);
    assert_eq!(by_id("nb-produce-128p").broker_partitions(), 128);
    assert_eq!(by_id("nb-produce-bulk").broker_partitions(), 6);
}

#[test]
fn generation_is_deterministic() {
    let cells = producer_cells();
    let bulk = cells.iter().find(|c| c.id == "nb-produce-bulk").unwrap();
    let first = generate(bulk);
    let second = generate(bulk);
    assert_eq!(first.len(), 20_000);
    assert_eq!(second.len(), 20_000);
    for (a, b) in first.iter().zip(second.iter()).step_by(997) {
        assert_eq!(a.id, b.id);
        assert_eq!(a.key, b.key);
        assert_eq!(a.value, b.value);
        assert_eq!(a.partition, b.partition);
    }
}

#[test]
fn ids_and_partitions_round_robin() {
    let cells = producer_cells();
    let bulk = cells.iter().find(|c| c.id == "nb-produce-bulk").unwrap();
    let records = generate(bulk);
    // 1-based contiguous IDs embedded as the value prefix.
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.id, i as u64 + 1);
        assert_eq!(&rec.value[..8], &(i as u64 + 1).to_be_bytes());
        assert_eq!(rec.partition, (i % 6) as i32);
        assert_eq!(rec.value.len(), 100);
        assert_eq!(rec.key.len(), 16);
    }
}

#[test]
fn mixed_topics_split_evenly() {
    let cells = producer_cells();
    let mixed = cells
        .iter()
        .find(|c| c.id == "nb-produce-mixed-topics")
        .unwrap();
    let records = generate(mixed);
    assert_eq!(records.len(), 12_000);
    let mut per_topic = [0usize; 3];
    for rec in &records {
        per_topic[rec.topic_idx] += 1;
        assert_eq!(rec.value.len(), 1024);
    }
    assert_eq!(per_topic, [4_000, 4_000, 4_000]);
    // Contiguous ID runs per topic in send order.
    assert_eq!(records[0].id, 1);
    assert_eq!(records[4_000].id, 4_001);
    assert_eq!(records[8_000].id, 8_001);
}
