//! KL04-09 wire-codec microbenchmarks (criterion): encode, decode, CRC
//! and compression at realistic fixture shapes. Timing only;
//! allocation counts live in the preflight census (deterministic).

use std::hint::black_box;
use std::path::PathBuf;

use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use partitionline::protocol::records::{decode_record_batch, encode_record_batch, Compression};

use codec::{
    build_records, compressed_batch, crc_body, load_fixtures, transform_fast, transform_slow,
};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

const SHAPES: &[(&str, usize, usize, usize, &str, usize)] = &[
    // (fixture, count, key_bytes, payload_bytes, entropy, headers)
    ("f01", 8, 16, 100, "random", 0),
    ("f02", 8, 16, 100, "text", 3),
    ("f03", 500, 16, 100, "random", 0),
    ("f04", 32, 16, 1024, "text", 2),
];

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");
    for (name, count, key, payload, entropy, headers) in SHAPES {
        let records = build_records(0xC0DEC, *count, *key, *payload, entropy, *headers);
        let batch = partitionline::protocol::records::RecordBatch::from_records(records);
        group.throughput(Throughput::Elements(*count as u64));
        group.bench_function(*name, |b| {
            b.iter(|| {
                let mut buf = BytesMut::new();
                encode_record_batch(&mut buf, black_box(&batch)).unwrap();
                black_box(buf)
            });
        });
    }
    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let fixtures = load_fixtures(&fixture_dir()).unwrap();
    let mut group = c.benchmark_group("decode");
    for fixture in &fixtures {
        group.throughput(Throughput::Elements(fixture.records as u64));
        group.bench_function(&fixture.name, |b| {
            b.iter_batched(
                || Bytes::from(fixture.bytes.clone()),
                |mut buf| decode_record_batch(black_box(&mut buf)).unwrap(),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_crc(c: &mut Criterion) {
    let fixtures = load_fixtures(&fixture_dir()).unwrap();
    let mut group = c.benchmark_group("crc32c");
    for fixture in &fixtures {
        let body = crc_body(&fixture.bytes).to_vec();
        group.throughput(Throughput::Bytes(body.len() as u64));
        group.bench_function(&fixture.name, |b| {
            b.iter(|| black_box(crc32c::crc32c(black_box(&body))));
        });
    }
    group.finish();
}

fn bench_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("compress");
    for (codec_name, codec) in [
        ("gzip", Compression::Gzip),
        ("snappy", Compression::Snappy),
        ("lz4", Compression::Lz4),
    ] {
        for entropy in ["random", "text"] {
            let records = build_records(0xC0DEC, 500, 16, 100, entropy, 0);
            let batch = compressed_batch(records, codec);
            let id = format!("{codec_name}/{entropy}");
            group.throughput(Throughput::Elements(500));
            group.bench_function(&id, |b| {
                b.iter(|| {
                    let mut buf = BytesMut::new();
                    encode_record_batch(&mut buf, black_box(&batch)).unwrap();
                    black_box(buf)
                });
            });
        }
    }
    group.finish();
}

fn bench_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("decompress");
    for (codec_name, codec) in [
        ("gzip", Compression::Gzip),
        ("snappy", Compression::Snappy),
        ("lz4", Compression::Lz4),
    ] {
        for entropy in ["random", "text"] {
            let records = build_records(0xC0DEC, 500, 16, 100, entropy, 0);
            let batch = compressed_batch(records, codec);
            let mut buf = BytesMut::new();
            encode_record_batch(&mut buf, &batch).unwrap();
            let bytes = buf.freeze().to_vec();
            let id = format!("{codec_name}/{entropy}");
            group.throughput(Throughput::Elements(500));
            group.bench_function(&id, |b| {
                b.iter_batched(
                    || Bytes::from(bytes.clone()),
                    |mut input| decode_record_batch(black_box(&mut input)).unwrap(),
                    BatchSize::SmallInput,
                );
            });
        }
    }
    group.finish();
}

fn bench_transforms(c: &mut Criterion) {
    let fixtures = load_fixtures(&fixture_dir()).unwrap();
    let f03 = fixtures.iter().find(|f| f.name == "f03").unwrap();
    let mut group = c.benchmark_group("transform");
    group.throughput(Throughput::Elements(f03.records as u64));
    group.bench_function("fast", |b| {
        b.iter(|| black_box(transform_fast(black_box(&f03.bytes)).unwrap()));
    });
    group.bench_function("slow", |b| {
        b.iter(|| black_box(transform_slow(black_box(&f03.bytes)).unwrap()));
    });
    group.finish();
}

// Separate family: existing cells and inputs remain unchanged.
fn bench_json1k(c: &mut Criterion) {
    use codec::json1k::{
        batch, raw_compress, raw_decompress, section, wire, CODECS, COUNTS, VALUE_BYTES,
    };
    for count in COUNTS {
        let input = section(count);
        for (name, compression) in CODECS {
            let batch = batch(count, compression);
            let encoded = wire(&batch);
            let packed = raw_compress(compression, &input);
            for operation in [
                "micro-compress",
                "micro-decompress",
                "raw-compress",
                "raw-decompress",
            ] {
                let mut group = c.benchmark_group(format!("{operation}:{name}:json1k"));
                group.throughput(Throughput::Bytes((count * VALUE_BYTES) as u64));
                group.bench_function(format!("{}k", count), |b| match operation {
                    "micro-compress" => b.iter(|| {
                        let mut output = BytesMut::new();
                        encode_record_batch(&mut output, black_box(&batch)).unwrap();
                        black_box(output);
                    }),
                    "micro-decompress" => b.iter_batched(
                        || encoded.clone(),
                        |mut input| {
                            black_box(decode_record_batch(black_box(&mut input)).unwrap());
                        },
                        BatchSize::SmallInput,
                    ),
                    "raw-compress" => b.iter(|| {
                        black_box(raw_compress(compression, black_box(&input)));
                    }),
                    "raw-decompress" => b.iter(|| {
                        black_box(raw_decompress(compression, black_box(&packed)));
                    }),
                    _ => unreachable!(),
                });
                group.finish();
            }
        }
    }
}

criterion_group!(
    benches,
    bench_encode,
    bench_decode,
    bench_crc,
    bench_compress,
    bench_decompress,
    bench_transforms,
    bench_json1k
);
criterion_main!(benches);
