//! Report zstd batch ratio, diagnostic timing and cold/warm allocation counts.
use bytes::BytesMut;
use codec::{
    census,
    zstd::{Case, SHAPES},
    CountingAlloc,
};
use serde_json::json;
use std::{hint::black_box, time::Instant};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let iterations = if arguments.is_empty() {
        3
    } else if arguments.len() == 2 && arguments[0] == "--iterations" {
        arguments[1].parse::<u32>()?
    } else {
        return Err("usage: zstd-census [--iterations 1..10000]".into());
    };
    if !(1..=10_000).contains(&iterations) {
        return Err("iterations out of bounds".into());
    }
    for &(name, count, key, payload, headers) in SHAPES {
        for entropy in ["random", "text"] {
            for level in [1, 3, 19] {
                let case = Case::new(name, count, key, payload, headers, entropy, level)?;
                let mut scratch = case.scratch()?;
                let mut out = BytesMut::new();
                case.encode(&mut out, &mut scratch)?;
                for operation in ["encode_cold", "encode_reused", "decode"] {
                    let (outcome, allocations, allocated_bytes) =
                        census(|| -> partitionline::Result<()> {
                            match operation {
                                "encode_cold" => {
                                    let mut cold = case.scratch()?;
                                    let mut output = BytesMut::new();
                                    case.encode(&mut output, &mut cold)?;
                                    black_box(output);
                                }
                                "encode_reused" => {
                                    out.clear();
                                    case.encode(&mut out, &mut scratch)?;
                                    black_box(&out);
                                }
                                _ => {
                                    black_box(case.decode()?);
                                }
                            }
                            Ok(())
                        });
                    outcome?;
                    // Timing is separate from the allocation instrumentation.
                    let start = Instant::now();
                    for _ in 0..iterations {
                        match operation {
                            "encode_cold" => {
                                let mut cold = case.scratch()?;
                                let mut output = BytesMut::new();
                                case.encode(black_box(&mut output), black_box(&mut cold))?;
                                black_box(output);
                            }
                            "encode_reused" => {
                                out.clear();
                                case.encode(black_box(&mut out), black_box(&mut scratch))?;
                                black_box(&out);
                            }
                            _ => {
                                black_box(case.decode()?);
                            }
                        }
                    }
                    let nanos = start.elapsed().as_nanos();
                    println!(
                        "{}",
                        json!({"schema_version":1,"case":case.name,"operation":operation,"backend":"zstd-rs0.1.0",
                        "encoder_level":level,"records":count,"payload_bytes":case.payload_bytes,"section_bytes":case.section_bytes,
                        "compressed_section_bytes":case.wire.len()-61,"compressed_over_uncompressed":(case.wire.len()-61) as f64/case.section_bytes as f64,
                        "iterations":iterations,"elapsed_ns":nanos,"payload_bytes_per_second":case.payload_bytes as f64*f64::from(iterations)*1e9/(nanos.max(1) as f64),
                        "allocations_per_operation":allocations,"requested_allocation_bytes_per_operation":allocated_bytes,
                        "preflight":"all record fields and complete input consumption","performance_claims_valid":false})
                    );
                }
            }
        }
    }
    Ok(())
}
