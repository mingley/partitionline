//! KL09-04 ratcheted allocation-budget gate for the codecs.
//!
//! Counting tool: [`codec::census`] over the [`codec::CountingAlloc`]
//! global allocator, reviewed under KL04-09. It lives only in this
//! workspace-excluded bench crate; the core crate gains no unsafe code
//! and no dev-dependency from this gate.
//!
//! Ratchet policy: a budget may be lowered by any card (an improvement
//! must update the consts below in the same commit) but raised only by
//! a separate maintainer-approved baseline-change card. A mismatch
//! fails the test and prints the full actual table to copy from.
//!
//! Single `#[test]` by design: the census counts process-wide
//! allocations and is only valid single-threaded, so all cells run
//! sequentially inside one test and no sibling test may exist in this
//! binary. Fixture bytes are built OUTSIDE the measured closure; only
//! the library call under test runs with the census enabled.

use std::hint::black_box;

use bytes::{Bytes, BytesMut};
use partitionline::protocol::api::{
    encode_produce_request, ProducePartitionData, ProduceTopicData,
};
use partitionline::protocol::records::{
    decode_record_batch, encode_record_batch, Compression, RecordBatch,
};

use codec::{build_records, census, compressed_batch, CountingAlloc};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

const SEED: u64 = 0xC0DEC;

/// (cell, budgeted allocs, budgeted bytes). Budgets equal the measured
/// counts on the day the gate landed; see the module docs for the
/// re-baseline policy.
const BUDGETS: &[(&str, u64, u64)] = &[
    ("micro-encode/1", 6, 504),
    ("micro-encode/1000", 15, 262_136),
    ("micro-compress/gzip", 21, 529_468),
    ("micro-compress/snappy", 14, 261_187),
    ("micro-compress/lz4", 16, 344_593),
    ("micro-request/v9", 17, 70_766),
    ("micro-decode/1", 2, 128),
    ("micro-decode/1000", 2, 104_024),
];

fn encode_batch(batch: &RecordBatch) -> (u64, u64) {
    let (_, allocs, bytes) = census(|| {
        let mut buf = BytesMut::new();
        encode_record_batch(black_box(&mut buf), black_box(batch)).unwrap();
        black_box(buf)
    });
    (allocs, bytes)
}

fn decode_bytes(bytes: &[u8]) -> (u64, u64) {
    let mut buf = Bytes::from(bytes.to_vec());
    let (batch, allocs, alloc_bytes) =
        census(|| black_box(decode_record_batch(black_box(&mut buf)).unwrap()));
    black_box(batch);
    (allocs, alloc_bytes)
}

fn batch_100b(n: usize) -> RecordBatch {
    RecordBatch::from_records(build_records(SEED, n, 16, 100, "random", 0))
}

fn batch_bytes(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = BytesMut::new();
    encode_record_batch(&mut buf, batch).unwrap();
    buf.to_vec()
}

fn measured() -> Vec<(&'static str, u64, u64)> {
    let mut rows = Vec::with_capacity(BUDGETS.len());

    for n in [1usize, 1000] {
        let batch = batch_100b(n);
        let (allocs, bytes) = encode_batch(&batch);
        rows.push((
            if n == 1 {
                "micro-encode/1"
            } else {
                "micro-encode/1000"
            },
            allocs,
            bytes,
        ));
    }

    for (name, codec) in [
        ("micro-compress/gzip", Compression::Gzip),
        ("micro-compress/snappy", Compression::Snappy),
        ("micro-compress/lz4", Compression::Lz4),
    ] {
        let records = build_records(SEED, 500, 16, 100, "text", 0);
        let batch = compressed_batch(records, codec);
        let (allocs, bytes) = encode_batch(&batch);
        rows.push((name, allocs, bytes));
    }

    {
        let batch = batch_100b(100);
        let topics = [ProduceTopicData {
            topic: "t".to_owned(),
            partitions: vec![ProducePartitionData {
                index: 0,
                records: batch,
            }],
        }];
        let (_, allocs, bytes) = census(|| {
            let mut buf = BytesMut::new();
            encode_produce_request(black_box(&mut buf), 9, None, 1, 30_000, black_box(&topics))
                .unwrap();
            black_box(buf)
        });
        rows.push(("micro-request/v9", allocs, bytes));
    }

    for n in [1usize, 1000] {
        let bytes = batch_bytes(&batch_100b(n));
        let (allocs, alloc_bytes) = decode_bytes(&bytes);
        rows.push((
            if n == 1 {
                "micro-decode/1"
            } else {
                "micro-decode/1000"
            },
            allocs,
            alloc_bytes,
        ));
    }

    rows
}

#[test]
fn alloc_budgets() {
    let rows = measured();
    assert_eq!(
        rows.len(),
        BUDGETS.len(),
        "cell count drift: update BUDGETS and measured() together"
    );
    let mut table = String::from("cell allocs bytes budget_allocs budget_bytes\n");
    let mut mismatches = 0usize;
    for (i, (name, allocs, bytes)) in rows.iter().enumerate() {
        let (want_name, want_allocs, want_bytes) = BUDGETS[i];
        assert_eq!(*name, want_name, "cell order drift at row {i}");
        table.push_str(&format!(
            "{name} {allocs} {bytes} {want_allocs} {want_bytes}\n"
        ));
        if *allocs != want_allocs || *bytes != want_bytes {
            mismatches += 1;
        }
    }
    println!("{table}");
    assert_eq!(
        mismatches, 0,
        "allocation budget mismatch ({mismatches} cells):\n{table}\
         Lower budgets in the improving commit; raises need a\n\
         maintainer-approved baseline-change card (KL09-04)."
    );
}
