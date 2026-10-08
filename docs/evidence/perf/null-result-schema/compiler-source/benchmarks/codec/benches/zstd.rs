//! Criterion timings; allocation counts and compression ratios are in zstd-census.
use bytes::BytesMut;
use codec::zstd::{Case, SHAPES};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;

fn bench(c: &mut Criterion) {
    for operation in ["encode_cold", "encode_reused", "decode"] {
        let mut group = c.benchmark_group(format!("zstd/{operation}"));
        for &(name, count, key, payload, headers) in SHAPES {
            for entropy in ["random", "text"] {
                for level in [1, 3, 19] {
                    let case =
                        Case::new(name, count, key, payload, headers, entropy, level).unwrap();
                    group.throughput(Throughput::Bytes(case.payload_bytes as u64));
                    let mut scratch = case.scratch().unwrap();
                    let mut out = BytesMut::new();
                    case.encode(&mut out, &mut scratch).unwrap();
                    group.bench_function(&case.name, |b| {
                        b.iter(|| match operation {
                            "encode_cold" => {
                                let mut cold = case.scratch().unwrap();
                                let mut output = BytesMut::new();
                                case.encode(black_box(&mut output), black_box(&mut cold))
                                    .unwrap();
                                black_box(output);
                            }
                            "encode_reused" => {
                                out.clear();
                                case.encode(black_box(&mut out), black_box(&mut scratch))
                                    .unwrap();
                                black_box(&out);
                            }
                            _ => {
                                black_box(case.decode().unwrap());
                            }
                        })
                    });
                }
            }
        }
        group.finish();
    }
}
criterion_group!(benches, bench);
criterion_main!(benches);
