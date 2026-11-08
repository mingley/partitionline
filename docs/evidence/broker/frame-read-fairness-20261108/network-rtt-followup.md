# Frame read loopback timing: individual RTT follow-up

Run [42469678575](https://github.com/mingley/partitionline/actions/runs/42469678575)
repeated the frame-read comparison and retained each measured request's RTT.
It is a follow-up to [the original frame-read report](README.md), not a broker
performance or production qualification.

## Scope and retained samples

- Baseline transport blob `e80acf8d5fe3614b35f079d3d6364d086b0abb84` from commit `60caa746c3654c89d01557e35e1100cdc2a84b74`; candidate transport blob `a36cf086c3d683a5f24b818705b15d8ce54245f4` from source commit `0a4c241a44f85bc47121b62fb67a6048d8b3f1fa`. The candidate blob was subsequently placed on main in `47df27f9`.
- Test-only workflow and fixture commit `9d5add8c3dc178dfb8a1701331fb9dd5d880c3ab`; appended fixture blob `0e95d63eb41a33b0be87dca4ea6ee073b7892db0`. Release binaries had SHA-256 `86f4864996fc2676256901d60b9561809abbce2ee5a68bcbc7e7f280619335fd` (baseline) and `b2be5a217b99883c94001f8bc6733c1f51d33e5e4cd44c394f932c2fccceed95` (candidate).
- Shared AMD EPYC 7763 runner, one pinned logical CPU, stable rustc 1.99.1 and LLVM 23.1.1. Six rounds alternate binary order; both binaries enable TLS. The loopback client and server use one current-thread Tokio runtime and TCP_NODELAY.
- A client sends a 4-byte length prefix and a body, then waits for a one-byte acknowledgement. The server reads the prefix from the wire into ReadAhead to model a prefetched prefix. The 4-KiB case uses a server-side adapter that caps each successful read at one byte; it is not a statement about actual packet sizes. Each case has 8 warmups followed by 128 measured 256-KiB frames or 256 measured 4-KiB frames.
- [Individual RTTs](network-rtt-samples.csv) contain 9,216 rows, in request order within each round/variant/case. [Round summaries](network-rtt-rounds.csv) contain 48 rows with total elapsed and the p50, p95, and p99 of that round. The CSV timestamps are when values were **printed**, after the timed interval; they are not request timestamps. Printing between cases can influence subsequent measurements.
- The 48 groups have the expected sequential sample indexes and counts. Recomputing p50/p95/p99 with index `floor((n-1) * percentile / 100)` from the individual RTTs matched all 48 emitted summaries.

## Results

Throughput is the bytes divided by the median of six per-round elapsed times.
Each latency is the median of six per-round percentile estimates, in
microseconds. It is not a pooled or fleet-level percentile.

| Case | Throughput baseline → candidate MiB/s | p50 baseline → candidate µs | p95 baseline → candidate µs | p99 baseline → candidate µs |
| --- | ---: | ---: | ---: | ---: |
| TCP, 256 KiB | 668.7 → 830.5 (+24.21%) | 363.9 → 286.8 | 397.0 → 384.9 | 473.2 → 507.5 |
| TLS/TCP, 256 KiB | 494.4 → 485.3 (-1.84%) | 484.6 → 491.9 | 842.2 → 848.6 | 851.3 → 858.7 |
| TCP, 4 KiB one-byte adapter | 1.085 → 1.081 (-0.34%) | 3,592.5 → 3,608.4 | 3,653.4 → 3,657.0 | 3,725.2 → 3,720.3 |
| TLS/TCP, 4 KiB one-byte adapter | 12.84 → 11.66 (-9.19%) | 289.9 → 319.5 | 380.3 → 393.1 | 488.6 → 547.8 |

The TCP 256-KiB outcome differs substantially from the earlier loopback run
[42469158690](https://github.com/mingley/partitionline/actions/runs/42469158690),
which measured 804.1 → 787.4 MiB/s (-2.08%) on an EPYC 9V74 runner with a
different test binary. That discrepancy is unexplained. Different hosts,
build layouts, and the additional sample printing outside intervals are
possible influences, but this test does not establish their contribution.
The data does not justify a general speedup claim.

These are sequential framing measurements with one connection, artificially
selected prefetch and fragmentation, and a small sample on a shared runner.
They do not measure complete Kafka handlers, sustained multi-client fairness,
or end-to-end production throughput. The candidate only bounds repeated
ready-read progress before requesting a yield; complete-frame shifts/copies,
one `poll_read`, allocation, and scheduler time remain outside that bound.
