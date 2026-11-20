# Producer sizing and timestamp scan

Batch sizing now calculates the maximum timestamp while it counts exact
record bytes. It uses the first record's timestamp as the base and keeps the
8,192-record pause. The writer still sets missing timestamps. The measured
length is the wire size for a plain batch and the unpacked section size for
a compressed batch.

The formatted source `0f6b6a8f892e0ea72e56b7674ce8a758a8cd4080` passed
Rustfmt, strict all-feature Clippy and 132 producer tests in each of
default, all-features and no-default. One test decodes and re-encodes a
mixed-timestamp fixture through classic, flexible and topic-ID Produce.
Existing tests verified both sizing and writing yields. The timing
candidate `b603b5db83b182f4ce18c34977cf0e3b88050b51` uses the same
production source. The format correction applies to a test import.
[Correctness run](https://github.com/mingley/partitionline/actions/runs/44640285852)

## Timing

The baseline was `7ad99cc16b94be7ac67b478ef8006d2f7e86e254`. Both arms
used stable Rust 1.100.0, LLVM 23.1.5, and default features plus Zstandard.
Eight pairs alternated order on one hosted x86_64 runner with one logical
CPU assigned to the timed process. The fixture checked every decoded
record and verified byte equality on re-encoding with a newer clock value.
Every pair and repeat matched the same complete request SHA-256 and byte
count for its shape.

The shapes use 1, 6 or 128 partitions and 1 to 8,193 records per partition.
Most records have patterned 100-byte values and 16-byte keys. The fixture
includes null keys and values and occasional headers. The timed path
encodes the complete Produce body. Fixture preparation, decoding and
digesting happen separately.

Changes are medians of eight paired ratios, with exact percentile 95%
bootstrap intervals. The calculation weights each resampled multiset by
its multiplicity across all 16,777,216 ordered eight-draw resamples.
Throughput holds each shape's encoded workload constant.
[Timing job](https://github.com/mingley/partitionline/actions/runs/44639969911/job/135968205258) ·
[224 raw measurements](raw-pairs.jsonl)

| Shape | Baseline median ns/request | Candidate median ns/request | Elapsed change and 95% interval | Throughput change and 95% interval | Slower pairs |
| --- | ---: | ---: | ---: | ---: | ---: |
| `v3-none-p1-n1` | 169.99 | 164.84 | −3.22% [−4.49% to −2.88%] | +3.33% [+2.96% to +4.70%] | 0/8 |
| `v9-none-p1-n1` | 176.07 | 168.60 | −4.26% [−4.52% to −3.88%] | +4.45% [+4.04% to +4.73%] | 0/8 |
| `v13-none-p1-n1` | 176.69 | 168.29 | −4.86% [−5.01% to −4.50%] | +5.10% [+4.71% to +5.27%] | 0/8 |
| `v3-none-p6-n64` | 19841.32 | 19835.56 | +0.03% [−0.57% to +0.47%] | −0.03% [−0.46% to +0.57%] | 4/8 |
| `v9-none-p6-n64` | 19836.06 | 19798.65 | −0.05% [−0.46% to +0.46%] | +0.05% [−0.46% to +0.46%] | 3/8 |
| `v13-none-p128-n16` | 113261.22 | 113654.12 | −0.10% [−0.24% to +1.06%] | +0.10% [−1.05% to +0.24%] | 3/8 |
| `v13-none-p1-n8193` | 517804.46 | 507761.61 | −1.93% [−2.34% to −1.44%] | +1.97% [+1.46% to +2.40%] | 0/8 |
| `v3-gzip-p1-n1` | 11691.02 | 11707.74 | +0.09% [+0.04% to +0.25%] | −0.09% [−0.25% to −0.04%] | 7/8 |
| `v3-snappy-p6-n64` | 39678.76 | 39395.91 | −0.64% [−1.03% to −0.21%] | +0.64% [+0.21% to +1.04%] | 1/8 |
| `v9-lz4-p6-n128` | 78787.64 | 78063.64 | −1.11% [−2.26% to −0.79%] | +1.13% [+0.80% to +2.32%] | 0/8 |
| `v13-snappy-p128-n4` | 58173.93 | 57624.95 | −0.80% [−1.33% to −0.46%] | +0.81% [+0.46% to +1.34%] | 1/8 |
| `v13-lz4-p128-n64` | 887411.60 | 848304.84 | −4.31% [−4.70% to −3.94%] | +4.50% [+4.10% to +4.93%] | 0/8 |
| `v7-zstd-p6-n128` | 199948.77 | 198400.91 | −0.68% [−1.00% to −0.02%] | +0.68% [+0.02% to +1.01%] | 1/8 |
| `v13-zstd-p128-n8` | 665004.55 | 666857.33 | +0.28% [−0.73% to +0.56%] | −0.28% [−0.55% to +0.73%] | 6/8 |

The one-record uncompressed cases and the 128-partition LZ4 case clear the
local timing criterion. One-record gzip took 0.09% longer in median paired
time, within the 2% guardrail. The results measure request encoding on a
shared runner.
[Local timing policy](https://github.com/mingley/partitionline/blob/7ad99cc16b94be7ac67b478ef8006d2f7e86e254/docs/plan/performance-leadership.md)

## Exact candidate integration checks

- The default and all-feature runs each passed both ordinary mock Produce
  cases, including one-record Produce and sending to known topics while
  waiting on metadata.
  [Mock and allocation jobs](https://github.com/mingley/partitionline/actions/runs/44642069000)
- The frozen allocation gate passed its three serial census cases and
  failure-preservation tests.
  [Allocation gate](https://github.com/mingley/partitionline/actions/runs/44642069000/job/135975358749)
- The pinned Apache Produce comparison and both committed corpus checks
  passed in the checkout and in the packaged crate with default and all
  features.
  [Wire qualification](https://github.com/mingley/partitionline/actions/runs/44642069000/job/135975358365)

The candidate recorded the same ten instruction counts as the baseline
source. The frozen gate still failed on LZ4 decoding at 920,136 instructions
against 903,610. That is a 1.83% increase against the 1% limit. The nine
other counters stayed within the limit.
[Baseline instruction failure](https://github.com/mingley/partitionline/actions/runs/44637814037/job/135960955415) ·
[Candidate instruction failure](https://github.com/mingley/partitionline/actions/runs/44642069000/job/135975358710)

```text
ci-perf-gate: x86_64-1.100.0 vs baseline @ 713c8a3d861a843bc1e6e229eecf53ce5632b3bf (tol +1%):
  iai_compress_gzip/text: Ir=11243402 vs 11243365 (+0.00%)
  iai_compress_lz4/text: Ir=2291524 vs 2291524 (+0.00%)
  iai_compress_snappy/text: Ir=1578383 vs 1578383 (+0.00%)
  iai_crc/f03: Ir=96702 vs 96702 (+0.00%)
  iai_decode/f03: Ir=368394 vs 368394 (+0.00%)
  iai_decompress_gzip/text: Ir=1174945 vs 1174945 (+0.00%)
  iai_decompress_lz4/text: Ir=920136 vs 903610 (+1.83%)
  iai_decompress_snappy/text: Ir=789514 vs 789514 (+0.00%)
  iai_encode/f03: Ir=779897 vs 779897 (+0.00%)
  iai_request/v9: Ir=195053 vs 195053 (+0.00%)
ci-perf-gate: FAIL
  iai_decompress_lz4/text: +1.83% exceeds +1% (Ir 920136 vs 903610)
```

## Build provenance

```text
SIZING_SOURCE arm=baseline blob=bc015178dada0a49c466d4d3b815434687e779ac instrumented_sha256=50e7dd218229ef715f529b849749c83a9a1a15455121684063f5f97ae2dc0523 production_sha256=8c6c5b39815c82c139a9ec473f6543eabd6577847814bfc886775aacc7af7458
SIZING_SOURCE arm=candidate blob=41c22d7d53b2c31a9397d2361d4833c2f0d04241 instrumented_sha256=308644979b763fa0184bda93fde9f1fb37d8cc7f381b4ede64ad990aed8784f9 production_sha256=ca20873419d82789c4f97e9ba1dfb86496466069d0f6965b6d3431ddf5c88c7b
SIZING_BUILD_FILE Cargo.toml sha256=cd14d7fa421e960e14e8dd86dde33b7f4dae5510fbcfca5ab2798ec062d2b677
SIZING_BUILD_FILE Cargo.lock sha256=a3873c6f1009d2742501943132c1dc17c3b7fbf4eee2bd8f2d907a9251146e4d
SIZING_HOST arch=x86_64 cpu=0 available=4
SIZING_BINARY baseline=aecf76d9d34ede0108017ebda971f7a3ac663e82adf3f08ecff53183f03d78d8 candidate=1effb8eaa92524ed21c925a730e6e4069968f9edd5744d3c0aafc79350e4f252
```
