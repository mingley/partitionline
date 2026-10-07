# Share acquisition lookup

`micro-share-ranges` calls the production ShareFetch lookup over 1,000
nonoverlapping three-offset ranges, with two-offset gaps. Each iteration
visits 5,000 offsets in order. A linear oracle verifies delivery counts
before timing. Fixture construction and 100 warmup iterations are excluded.

```bash
cargo +stable build --locked --release --manifest-path benchmarks/codec/Cargo.toml --bin share-ranges
taskset -c 2 target/release/share-ranges --iterations 10000 --output result.json
```

Use the binary location reported by Cargo when setting `CARGO_TARGET_DIR`.
The result contains elapsed time, time per lookup, checksums and a separate
allocation census. It measures range lookup, not complete ShareFetch throughput.
Qualification uses at least five paired A/B repetitions on the same host,
with retained source and binary hashes and bootstrap confidence intervals.
