# Transaction response decode experiment

An isolated candidate reuses the first decoded transaction response instead of
decoding it twice. It remains unapplied: it preserves the existing
AddPartitions response-coverage and error-precedence limitations, and its
changes overlap the transaction deadline and coverage work.

The [x86 loopback run](https://github.com/mingley/partitionline/actions/runs/42439775231)
compared base `3f45fe6255e72abc6b0c4488bc13117779c50787` with
[`e847fb95a4da6b60cda38620873f55110faa057c`](https://github.com/mingley/partitionline/commit/e847fb95a4da6b60cda38620873f55110faa057c).
It used separate target directories and distinct binary hashes, stable Rust
1.99.1, release thin LTO and one codegen unit on one pinned vCPU of a shared
AMD EPYC x86 runner. Both optimized test binaries passed selected malformed
response and coordinator-retry controls.

The table shows the median of six paired candidate/baseline ratios for
per-request loopback latency; lower is faster. Each process discarded 40
warmups and measured 300 requests.

| Partitions in response | v0 | v3 |
| ---: | ---: | ---: |
| 1 | 1.001 | 0.991 |
| 128 | 0.967 | 0.945 |
| 8,192 | 0.683 | 0.616 |

One v0 large-response pair had a ratio of 0.971, so host noise matters. The
test uses a loopback responder and optimized test binaries, not a production
Kafka client/server throughput comparison. The job log retains the 21,600
samples and per-pair p95 values. The earlier
[attempt](https://github.com/mingley/partitionline/actions/runs/42439141699)
reused the baseline binary and produced no valid paired result.

A combined implementation must validate every requested partition and preserve
error precedence, cancellation and cooperative scheduling. Re-run correctness
and matched performance tests against its exact source before adoption. These
measurements do not establish a fastest-client claim.
