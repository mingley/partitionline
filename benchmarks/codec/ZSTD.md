# Zstd batch benchmarks

These tools measure the client record-batch encoder and decoder with
`zstd-rs 0.1.0`. They use levels 1, 3 and 19, random and text payloads, and
four batch shapes: small 100-byte records, bulk 100-byte records, 1 KiB records
with headers, and 64 KiB records. Each workload verifies every decoded record
and metadata field before measurement.

Run with latest stable Rust:

```sh
cargo +stable test --locked --manifest-path benchmarks/codec/Cargo.toml --features zstd
cargo +stable run --locked --release --manifest-path benchmarks/codec/Cargo.toml --features zstd --bin zstd-census -- --iterations 3
cargo +stable bench --locked --manifest-path benchmarks/codec/Cargo.toml --features zstd --bench zstd
```

`zstd-census` emits 72 JSON rows: 24 workloads with fresh-context encoding,
reused-context encoding and decoding. It reports compressed/uncompressed
record-section ratio, visible payload bytes per second, allocation calls and
requested allocation bytes. Kafka's fixed 61-byte batch header is excluded
from the ratio. Requested allocation bytes are not live memory or RSS.
Record construction and validation are outside measured operations. Reused
encoding includes output-buffer reuse; fresh encoding includes new context
and output allocation. Decode clones immutable wire backing without copying.

Allocation instrumentation runs separately from timing, on one thread. The
existing counting allocator wraps the system allocator in this benchmark
crate. The client has no allocator instrumentation. Census timings are short
diagnostics; Criterion retains statistical timing results for longer runs.
Neither tool establishes a peer comparison or a performance ranking.

The scenario registry requires level-3 zstd bulk and fetch cells. Comparisons
must record the actual encoder backend/version, level and compressed batch
sizes for each peer. Decoder timings use the same pre-encoded bytes; decoder
level is not a setting. Rust uses `zstd-rs`; Java uses its pinned zstd-jni/libzstd
build, and librdkafka uses its resolved libzstd build. An unknown backend,
different encoder level or unavailable codec makes a comparison ineligible.
The Go peer uses klauspost/compress; its current default level mapping must be
resolved explicitly before it can enter the level-3 comparison.
All required cells remain unexecuted until a complete matched campaign.
