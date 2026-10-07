//! Native clock and allocation measurements for the exact Iai Produce v9 shape.
use bytes::BytesMut;
use codec::{build_records, census, CountingAlloc};
use partitionline::protocol::{
    api::{encode_produce_request, ProducePartitionData, ProduceTopicData},
    records::RecordBatch,
};
use std::{hint::black_box, io::Write, time::Instant};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 1 {
        return Err("usage: baseline-request fresh-wire-output-path".into());
    }
    let topics = vec![ProduceTopicData {
        topic: "t".into(),
        partitions: vec![ProducePartitionData {
            index: 0,
            records: RecordBatch::from_records(build_records(0xC0DEC, 100, 16, 100, "random", 0)),
        }],
    }];
    let encode = || {
        let mut output = BytesMut::new();
        encode_produce_request(&mut output, 9, None, 1, 30_000, black_box(&topics)).unwrap();
        black_box(output)
    };
    let wire = encode();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args[0])?;
    file.write_all(&wire)?;
    file.sync_all()?;
    for _ in 0..10_000 {
        black_box(encode());
    }
    let (_, allocation_count, allocated_bytes) = census(encode);
    let mut samples = Vec::with_capacity(30);
    for _ in 0..30 {
        let start = Instant::now();
        for _ in 0..10_000 {
            black_box(encode());
        }
        let elapsed = start.elapsed().as_nanos();
        samples.push(elapsed as f64 / 10_000.0);
    }
    println!(
        "{}",
        serde_json::json!({"schema_version":1,"cell":"micro-request",
        "scope":"local/unsigned","suite_hold":"active","api_key":0,"version":9,
        "records":100,"key_bytes":16,"value_bytes":100,"headers":0,"entropy":"random",
        "topic":"t","partition":0,"acks":1,"timeout_ms":30000,"seed":0xC0DECu64,
        "wire_bytes":wire.len(),"native_samples_ns_per_operation":samples,
        "iterations_per_sample":10000,"samples":30,"warmup_iterations":10000,
        "allocations_per_operation":allocation_count,"allocated_bytes_per_operation":allocated_bytes,
        "measurement_note":"Exact Iai request setup; fresh output buffer per operation. Setup, warmup, allocation census and wire retention excluded from native timing."})
    );
    Ok(())
}
