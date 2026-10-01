# KL10-03 local JSON codec baseline

Benchmark code: `312f742b65f224e5de70896c7d823789eca4a7bd`. Linux x86_64, Rust 1.98.1, Valgrind 3.24.0,
iai-callgrind 0.14.2; shared cloud host with no exposed frequency governor.
Local unsigned initial baseline; no A/B optimization or peer comparison.

The full requested command completed all 24 cells:

```bash
IAI_CALLGRIND_ALLOW_ASLR=yes cargo bench --locked --manifest-path benchmarks/codec/Cargo.toml -- json1k
cargo bench --locked --manifest-path benchmarks/codec/Cargo.toml --bench json1k_iai -- --allow-aslr --output-format=json
python3 -B benchmarks/codec/validate-json1k.py
```

Each Criterion cell retains 100 raw samples and its bootstrap 95% confidence
interval. Every instruction count matched exactly in a second run. Debug and
release allocation censuses matched exactly. `host.json` records the source,
tools and historical baseline checksums. Those baseline files were unchanged.
`bench-output.txt` is the complete successful filter run; `instructions-repeat.jsonl`
is the structured second run; `callgrind-first/*.gz` retains the first run's raw
counters. Absolute workspace paths are replaced with descriptive placeholders.

Batch labels mean 16/256 KiB of values; keys and Kafka metadata add overhead.
Input section SHA-256 and compressed/section ratios are recorded per cell.
Gzip ratios are about 0.252/0.209, LZ4 0.417/0.392, Snappy 0.351/0.330.
See [workload and measurement boundaries](../../../../benchmarks/codec/JSON1K.md).
Raw backends receive identical serialized record-section bytes in one call;
Snappy's raw output omits the 20-byte Xerial prefix. Full vs raw also differs
in buffer sizing, CRC/parsing and expansion checks, so subtraction does not
isolate a single overhead. Owned setup inputs are dropped within IAI functions;
construction is excluded. Raw decode is a trusted-fixture benchmark.

The validator rejects missing cells, invalid ratios and zero instruction counts.
These local measurements qualify the builder card, not a performance leadership
claim, an arm64 baseline or a controlled-host production campaign.
