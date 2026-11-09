//! Comparative complete Produce request encoding fixture.
use std::error::Error;
use std::fs::File;
use std::hint::black_box;
use std::io::{BufWriter, Write};
use std::task::{Context, Poll, Waker};
use std::time::Instant;
use std::future::Future;

use bytes::{Buf, Bytes, BytesMut};
use partitionline::protocol::api::{
    decode_produce_request, decode_produce_request_with_topic_ids, encode_produce_request,
    encode_produce_request_cooperative, encode_produce_request_with_topic_ids,
    ProducePartitionData, ProduceTopicData,
};
use partitionline::protocol::records::{Header, Record, RecordBatch};

#[derive(Clone, Copy)]
struct Shape {
    name: &'static str, partitions: usize, records: usize, value: usize,
    headers: usize, codec: i16, version: i16, cooperative: bool, cold: bool,
}

const SHAPES: &[Shape] = &[
    Shape { name: "one_empty", partitions: 1, records: 0, value: 0, headers: 0, codec: 0, version: 9, cooperative: false, cold: false },
    Shape { name: "one_record", partitions: 1, records: 1, value: 100, headers: 0, codec: 0, version: 9, cooperative: false, cold: false },
    Shape { name: "eight_records", partitions: 1, records: 8, value: 100, headers: 0, codec: 0, version: 9, cooperative: false, cold: false },
    Shape { name: "five_hundred", partitions: 1, records: 500, value: 100, headers: 0, codec: 0, version: 9, cooperative: false, cold: false },
    Shape { name: "header_heavy", partitions: 1, records: 128, value: 100, headers: 8, codec: 0, version: 9, cooperative: false, cold: false },
    Shape { name: "legacy", partitions: 1, records: 128, value: 100, headers: 0, codec: 0, version: 7, cooperative: false, cold: false },
    Shape { name: "topic_id", partitions: 1, records: 128, value: 100, headers: 0, codec: 0, version: 13, cooperative: false, cold: false },
    Shape { name: "multi", partitions: 8, records: 8, value: 100, headers: 0, codec: 0, version: 9, cooperative: false, cold: false },
    Shape { name: "many_empty", partitions: 64, records: 0, value: 0, headers: 0, codec: 0, version: 9, cooperative: false, cold: false },
    Shape { name: "mixed_compression", partitions: 4, records: 128, value: 100, headers: 0, codec: -1, version: 9, cooperative: false, cold: false },
    Shape { name: "gzip", partitions: 1, records: 128, value: 100, headers: 0, codec: 1, version: 9, cooperative: false, cold: false },
    Shape { name: "snappy", partitions: 1, records: 128, value: 100, headers: 0, codec: 2, version: 9, cooperative: false, cold: false },
    Shape { name: "lz4", partitions: 1, records: 128, value: 100, headers: 0, codec: 3, version: 9, cooperative: false, cold: false },
    Shape { name: "async_eight", partitions: 1, records: 8, value: 100, headers: 0, codec: 0, version: 9, cooperative: true, cold: false },
    Shape { name: "async_five_hundred", partitions: 1, records: 500, value: 100, headers: 0, codec: 0, version: 9, cooperative: true, cold: false },
    Shape { name: "async_multi", partitions: 8, records: 8, value: 100, headers: 0, codec: 0, version: 9, cooperative: true, cold: false },
    Shape { name: "cold_one", partitions: 1, records: 1, value: 100, headers: 0, codec: 0, version: 9, cooperative: false, cold: true },
];

fn data(i: usize, len: usize) -> Bytes {
    Bytes::from((0..len).map(|n| ((i.wrapping_mul(37) + n.wrapping_mul(53)) & 255) as u8).collect::<Vec<_>>())
}

fn topics(shape: Shape) -> Vec<ProduceTopicData> {
    let mut parts = Vec::new();
    for p in 0..shape.partitions {
        let mut records = Vec::new();
        for i in 0..shape.records {
            let headers = (0..shape.headers).map(|h| Header {
                key: format!("h{h}-é"), value: if (h+i)%3 == 0 { None } else { Some(data(i+h, 17)) },
            }).collect();
            records.push(Record {
                offset: i as i64, timestamp: (i % 31) as i64,
                key: match i % 3 { 0 => None, 1 => Some(Bytes::new()), _ => Some(data(i, 16)) },
                value: if i != 0 && i % 7 == 0 { None } else { Some(data(i, shape.value)) }, headers,
            });
        }
        let mut batch = RecordBatch::from_records(records);
        batch.attributes = if shape.codec == -1 { if p % 2 == 0 { 1 } else { 0 } } else { shape.codec };
        parts.push(ProducePartitionData { index: p as i32, records: batch });
    }
    vec![ProduceTopicData { topic: "orders-é".into(), partitions: parts }]
}

fn ids(topics: &[ProduceTopicData]) -> Vec<(String, [u8; 16])> {
    topics.iter().enumerate().map(|(i, t)| (t.topic.clone(), ((i + 1) as u128).to_be_bytes())).collect()
}

fn encode(
    shape: Shape, dst: &mut BytesMut, topics: &[ProduceTopicData],
    identities: &[(String, [u8; 16])], runtime: &tokio::runtime::Runtime,
) -> Result<(), String> {
    if shape.cooperative {
        runtime.block_on(encode_produce_request_cooperative(dst, shape.version, Some("tx"), -1, 1500, topics))
            .map_err(|e| e.to_string())
    } else if shape.version == 13 {
        encode_produce_request_with_topic_ids(dst, Some("tx"), -1, 1500, topics, identities).map_err(|e| e.to_string())
    } else {
        encode_produce_request(dst, shape.version, Some("tx"), -1, 1500, topics).map_err(|e| e.to_string())
    }
}

fn emit(out: &mut BufWriter<File>, label: &str, result: Result<(), String>, dst: &[u8]) -> Result<(), Box<dyn Error>> {
    let status = match result { Ok(()) => "ok".to_owned(), Err(error) => format!("error:{error}") };
    for part in [label.as_bytes(), status.as_bytes(), dst] {
        out.write_all(&(part.len() as u64).to_le_bytes())?;
        out.write_all(part)?;
    }
    Ok(())
}

fn verify_success(shape: Shape, dst: &[u8], source: &[ProduceTopicData], identities: &[(String, [u8; 16])]) -> Result<(), Box<dyn Error>> {
    let mut input = &dst[6..];
    let (tx, acks, timeout, decoded, wire_ids) = if shape.version == 13 {
        decode_produce_request_with_topic_ids(&mut input)?
    } else {
        let (tx, acks, timeout, decoded) = decode_produce_request(&mut input, shape.version)?;
        (tx, acks, timeout, decoded, Vec::new())
    };
    assert_eq!(tx.as_deref(), Some("tx"));
    assert_eq!(acks, -1);
    assert_eq!(timeout, 1500);
    assert_eq!(input.remaining(), 0);
    assert_eq!(decoded.len(), source.len());
    if shape.version == 13 { assert_eq!(wire_ids, identities.iter().map(|(_, id)| *id).collect::<Vec<_>>()); }
    for (actual, expected) in decoded.iter().zip(source) {
        if shape.version != 13 { assert_eq!(actual.topic, expected.topic); }
        assert_eq!(actual.partitions.len(), expected.partitions.len());
        for (a, b) in actual.partitions.iter().zip(&expected.partitions) {
            assert_eq!(a.index, b.index);
            assert_eq!(a.records.records, b.records.records);
        }
    }
    Ok(())
}

fn corpus(path: &str, runtime: &tokio::runtime::Runtime) -> Result<(), Box<dyn Error>> {
    let mut out = BufWriter::new(File::create(path)?);
    let mut cases = 0usize;
    // Different lengths matter: the cached length must follow the first
    // uncompressed partition, not the first partition or first topic.
    for version in 3..=13 {
        for pattern in 0..6 {
            let specifications: Vec<(usize, usize, usize, i16)> = match pattern {
                0 => vec![(1, 17, 0, 1), (9, 701, 2, 0)],
                1 => vec![(3, 31, 1, 0), (2, 511, 0, 1)],
                2 => vec![(0, 0, 0, 0), (2, 333, 1, 1), (7, 11, 0, 0)],
                3 => vec![(0, 0, 0, 1), (2, 333, 1, 1), (7, 11, 0, 0)],
                4 => vec![(1, 16, 0, 3), (13, 123, 3, 0), (0, 0, 0, 2)],
                _ => vec![(1, 19, 1, 2), (0, 0, 0, 0), (3, 511, 2, 0), (2, 63, 1, 3)],
            };
            let mut parts = Vec::new();
            for (index, &(count, value, headers, codec)) in specifications.iter().enumerate() {
                let shape = Shape {
                    partitions: 1, records: count, value, headers, codec, version,
                    ..SHAPES[0]
                };
                let mut one = topics(shape).remove(0).partitions.remove(0);
                one.index = index as i32;
                parts.push(one);
            }
            let arranged = vec![
                ProduceTopicData { topic: "empty-topic".into(), partitions: Vec::new() },
                ProduceTopicData { topic: "mixed-topic".into(), partitions: parts },
            ];
            let shape = Shape { version, ..SHAPES[0] };
            let identities = ids(&arranged);
            let mut dst = BytesMut::from(&b"before"[..]);
            let result = encode(shape, &mut dst, &arranged, &identities, runtime);
            if result.is_err() { return Err(format!("ordered mix {version}/{pattern}: {result:?}").into()); }
            verify_success(shape, &dst, &arranged, &identities)?;
            if version != 13 {
                let mut cooperative = BytesMut::from(&b"before"[..]);
                let other = encode(Shape { cooperative: true, ..shape }, &mut cooperative, &arranged, &identities, runtime);
                assert_eq!(result, other);
                assert_eq!(dst, cooperative);
            }
            emit(&mut out, &format!("ordered-{version}-{pattern}"), result, &dst)?;
            cases += 1;
        }
    }
    for version in 3..=13 {
        for &shape in SHAPES.iter().filter(|s| !s.cooperative && !s.cold) {
            let shape = Shape { version, ..shape };
            let topics = topics(shape);
            let identities = ids(&topics);
            let mut dst = BytesMut::from(&b"before"[..]);
            let result = encode(shape, &mut dst, &topics, &identities, runtime);
            if result.is_err() { return Err(format!("unexpected error {version} {}: {result:?}", shape.name).into()); }
            verify_success(shape, &dst, &topics, &identities)?;
            if version != 13 {
                let mut cooperative = BytesMut::from(&b"before"[..]);
                let result2 = encode(Shape { cooperative: true, ..shape }, &mut cooperative, &topics, &identities, runtime);
                assert_eq!(result, result2);
                assert_eq!(dst, cooperative, "sync/cooperative mismatch");
            }
            emit(&mut out, &format!("{version}:{}", shape.name), result, &dst)?;
            cases += 1;
        }
    }
    for version in [3, 9, 13] {
        let shape = Shape { version, ..SHAPES[2] };
        for arrangement in 0..3 {
            let mut input_topics = topics(shape);
            match arrangement {
                0 => input_topics.clear(),
                1 => input_topics[0].partitions.clear(),
                _ => {
                    let mut later = topics(Shape { partitions: 3, codec: -1, ..shape }).remove(0);
                    later.topic = "billing".into();
                    input_topics[0].partitions.clear();
                    input_topics.push(later);
                }
            }
            let identities = ids(&input_topics);
            let mut dst = BytesMut::from(&b"before"[..]);
            let result = encode(shape, &mut dst, &input_topics, &identities, runtime);
            if result.is_err() { return Err(format!("arrangement error: {result:?}").into()); }
            verify_success(shape, &dst, &input_topics, &identities)?;
            if version != 13 {
                let mut cooperative = BytesMut::from(&b"before"[..]);
                let other = encode(Shape { cooperative: true, ..shape }, &mut cooperative, &input_topics, &identities, runtime);
                assert_eq!(result, other);
                assert_eq!(dst, cooperative);
            }
            emit(&mut out, &format!("arrangement-{version}-{arrangement}"), result, &dst)?;
            cases += 1;
        }
    }
    let shape = SHAPES[3];
    for version in [3, 7, 9, 12, 13] {
        for variant in 0..6 {
            let mut topics = topics(Shape { version, partitions: 3, ..shape });
            match variant {
                0 => topics[0].partitions[0].records.attributes = 7,
                1 => topics[0].partitions[2].records.attributes = 7,
                2 => { topics[0].partitions[0].records.base_timestamp = i64::MIN; topics[0].partitions[0].records.records[0].timestamp = i64::MAX; },
                3 => { topics[0].partitions[2].records.base_timestamp = i64::MIN; topics[0].partitions[2].records.records[0].timestamp = i64::MAX; },
                4 => topics[0].topic = "x".repeat(40_000),
                _ => topics[0].partitions[0].records.base_offset = i64::MAX,
            }
            let identities = ids(&topics);
            let mut dst = BytesMut::from(&b"before"[..]);
            let shape = Shape { version, ..shape };
            let result = encode(shape, &mut dst, &topics, &identities, runtime);
            if version != 13 {
                let mut cooperative = BytesMut::from(&b"before"[..]);
                let result2 = encode(Shape { cooperative: true, ..shape }, &mut cooperative, &topics, &identities, runtime);
                assert_eq!(result, result2);
                assert_eq!(dst, cooperative);
            }
            emit(&mut out, &format!("error-{version}-{variant}"), result, &dst)?;
            cases += 1;
        }
    }
    // Drop the future at each of its first three yield boundaries: compression
    // scan, request planning and request writing.
    let shape = Shape { partitions: 8_200, records: 0, ..SHAPES[0] };
    let many = topics(shape);
    let _guard = runtime.enter();
    for polls in 1..=3 {
        let mut dst = BytesMut::from(&b"before"[..]);
        {
            let mut future = Box::pin(encode_produce_request_cooperative(&mut dst, 9, Some("tx"), -1, 1500, &many));
            let mut context = Context::from_waker(Waker::noop());
            for _ in 0..polls {
                match future.as_mut().poll(&mut context) {
                    Poll::Pending => (),
                    Poll::Ready(value) => return Err(format!("completed before cancellation: {value:?}").into()),
                }
            }
        }
        emit(&mut out, &format!("cancel-{polls}"), Ok(()), &dst)?;
        cases += 1;
    }
    out.flush()?;
    eprintln!("corpus cases={cases}");
    Ok(())
}

fn bench(shape: Shape, iterations: u64, runtime: &tokio::runtime::Runtime) -> Result<u128, Box<dyn Error>> {
    let topics = topics(shape);
    let identities = ids(&topics);
    let mut dst = BytesMut::new();
    let begin = Instant::now();
    for _ in 0..iterations {
        if shape.cold { dst = BytesMut::new(); } else { dst.clear(); }
        encode(shape, &mut dst, black_box(&topics), black_box(&identities), runtime)?;
        let _ = black_box(&dst);
    }
    Ok(begin.elapsed().as_nanos())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    if args.get(1).map(String::as_str) == Some("corpus") {
        return corpus(args.get(2).ok_or("missing corpus path")?, &runtime);
    }
    let shape_name = args.get(2).ok_or("missing shape")?;
    let shape = *SHAPES.iter().find(|s| s.name == shape_name).ok_or("unknown shape")?;
    match args.get(1).map(String::as_str) {
        Some("calibrate") => {
            let mut count = 1u64;
            while bench(shape, count, &runtime)? < 30_000_000 && count < 16_777_216 { count *= 2; }
            println!("{count}");
        }
        Some("bench") => {
            let count = args.get(3).ok_or("missing iterations")?.parse::<u64>()?;
            let elapsed = bench(shape, count, &runtime)?;
            println!("{},{},{}", shape.name, count, elapsed);
        }
        _ => return Err("expected corpus, calibrate or bench".into()),
    }
    Ok(())
}
