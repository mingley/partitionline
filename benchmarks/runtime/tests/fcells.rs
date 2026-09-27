//! Fetch-cell contract tests (KL09-10): the seven section-4
//! consumer cells exist exactly as defined, and the ID/hash verifier
//! accepts genuine synth records while rejecting corruptions.

use std::time::Duration;

use bytes::Bytes;
use partitionline::consumer::FetchedRecord;

use nullbroker::synth::record_hash;
use runtime::fcells::{fetch_cells, verify_record};

#[test]
fn seven_cells_exact_ids_and_shapes() {
    let cells = fetch_cells();
    let ids: Vec<&str> = cells.iter().map(|c| c.id).collect();
    assert_eq!(
        ids,
        vec![
            "nb-fetch-bulk",
            "nb-fetch-1000p",
            "nb-fetch-committed-aborts",
            "nb-fetch-seek-in-batch",
            "nb-fetch-capped-paused",
            "nb-fetch-appdelay",
            "nb-fetch-multinode",
        ]
    );
    let by_id = |id: &str| cells.iter().find(|c| c.id == id).unwrap();
    let bulk = by_id("nb-fetch-bulk");
    assert_eq!((bulk.partitions, bulk.synth_payload_bytes), (6, 100));
    assert_eq!(bulk.target_records, 20_000);
    assert!(!bulk.read_committed);

    let k = by_id("nb-fetch-1000p");
    assert_eq!(k.partitions, 1000);
    assert_eq!(k.synth_records_per_partition, 10);
    assert_eq!(k.target_records, 10_000);

    let aborts = by_id("nb-fetch-committed-aborts");
    assert!(aborts.read_committed);
    assert_eq!(aborts.synth_abort_every, 5);

    let seek = by_id("nb-fetch-seek-in-batch");
    assert_eq!(seek.synth_records_per_batch, 1000);
    assert_eq!(seek.seek_offset, Some(500));

    let capped = by_id("nb-fetch-capped-paused");
    assert_eq!(capped.max_poll_records, Some(1));
    assert_eq!(capped.paused_partitions, vec![1, 2, 3, 4, 5]);
    // 100k-record paused backlog: 5 partitions x 20k.
    assert_eq!(capped.synth_records_per_partition, 20_000);

    let app = by_id("nb-fetch-appdelay");
    assert_eq!(app.app_delay_per_batch, Duration::from_millis(1));

    let multi = by_id("nb-fetch-multinode");
    assert_eq!(multi.nodes, 3);
    assert_eq!(multi.slow_node, Some(2));
    assert_eq!(multi.slow_delay, Duration::from_millis(50));
}

#[test]
fn retry_cell_shape() {
    let cell = runtime::cells::retry_cell();
    assert_eq!(cell.id, "nb-produce-retry");
    assert_eq!(cell.total_records(), 20_000);
    assert_eq!(cell.acks, 1);
    assert_eq!(cell.mode, runtime::cells::DriveMode::Pipelined);
}

fn synth_record(seed: u64, partition: i32, offset: u64) -> FetchedRecord {
    let hash = record_hash(seed, partition, offset);
    let mut value = Vec::with_capacity(100);
    value.extend_from_slice(&offset.to_be_bytes());
    value.extend_from_slice(&hash.to_be_bytes());
    // Mirror synth encode_value filler exactly (words + tail).
    let mut filler_len = 100 - 16;
    let mut word = hash;
    while filler_len >= 8 {
        word = nullbroker::synth::splitmix64(word);
        value.extend_from_slice(&word.to_be_bytes());
        filler_len -= 8;
    }
    if filler_len > 0 {
        word = nullbroker::synth::splitmix64(word);
        value.extend_from_slice(&word.to_be_bytes()[..filler_len]);
    }
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&partition.to_be_bytes());
    key.extend_from_slice(&offset.to_be_bytes());
    key.extend_from_slice(&(hash as u32).to_be_bytes());
    FetchedRecord {
        topic: "t".to_owned(),
        partition,
        offset: offset as i64,
        timestamp: 0,
        timestamp_type: partitionline::protocol::records::TimestampType::CreateTime,
        key: Some(Bytes::from(key)),
        value: Some(Bytes::from(value)),
        headers: Vec::new(),
        leader_epoch: None,
    }
}

#[test]
fn verifier_accepts_genuine_records() {
    for partition in [0, 2, 5] {
        for offset in [0u64, 1, 499, 500, 12_345] {
            let rec = synth_record(0xFE7C_0001, partition, offset);
            verify_record(0xFE7C_0001, 100, &rec).expect("genuine record verifies");
        }
    }
}

#[test]
fn verifier_rejects_corruption() {
    // Wrong seed.
    let rec = synth_record(0xFE7C_0001, 0, 42);
    assert!(verify_record(0xBEEF, 100, &rec).is_err());
    // Value ID disagrees with the consumer offset.
    let mut rec = synth_record(0xFE7C_0001, 1, 7);
    rec.offset = 8;
    assert!(verify_record(0xFE7C_0001, 100, &rec).is_err());
    // Flipped hash byte.
    let mut rec = synth_record(0xFE7C_0001, 1, 7);
    let mut value = rec.value.clone().unwrap().to_vec();
    value[9] ^= 0xff;
    rec.value = Some(Bytes::from(value));
    assert!(verify_record(0xFE7C_0001, 100, &rec).is_err());
    // Short value.
    let mut rec = synth_record(0xFE7C_0001, 1, 7);
    rec.value = Some(Bytes::from(vec![0u8; 50]));
    assert!(verify_record(0xFE7C_0001, 100, &rec).is_err());
    // Missing key.
    let mut rec = synth_record(0xFE7C_0001, 1, 7);
    rec.key = None;
    assert!(verify_record(0xFE7C_0001, 100, &rec).is_err());
}
