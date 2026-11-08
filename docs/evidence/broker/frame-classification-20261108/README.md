# Buffered frame classification experiment

The draft removes a repeated logical classification from an incomplete
buffered-frame read. It has **not been applied to main**: matched loopback
measurements showed a material TCP regression, and a full broker gate was not
run. The result does not support a Kafka or broker speedup claim.

## Source and behavior

- Baseline main commit `e487f7294773f75112f15437992ed780b53c7307`, transport blob `a36cf086c3d683a5f24b818705b15d8ce54245f4`.
- Production draft commit `6c92c25c995abf7bb65c3191f708fda617485ce7`, transport blob `95a7e220ae8a6e34a0ae41936c3b36c7bc596ade`; this is the source used in both timing runs.
- The same draft with formatted, permanent-style retention and invalid-prefix tests is isolated at `fa5ae4908aa6a52ccdc6dab80de1f936b9bc8d47`, transport blob `60de0562040c7e5689c7ae3f1d9dce2ab515de78`. It has not had a full broker qualification.
- In the baseline loop, `fill_complete_frame` classifies an incomplete frame and a second `classify_ahead` call obtains its read budget. The draft classifies once on that incomplete path. Complete-frame extraction, allocation ownership, byte and read-count yield thresholds, and deadline checks are otherwise unchanged.
- This does not address `fill_complete_frame`'s synchronous prefix removal, shift or payload copy. A complete buffered frame can still require work proportional to its size in one poll. The public handler currently takes an owned `Vec<u8>` starting at the body; removing that copy/shift requires a separate design that preserves pipeline state, cancellation and credential wiping.

A test-only atomic counter in [42474130366](https://github.com/mingley/partitionline/actions/runs/42474130366)
confirmed logical classifications for equal bodies/read counts: baseline →
draft was 4 → 4 for 32 bytes in one read, 19 → 12 for 64 KiB in 8 KiB reads,
and 8,226 → 4,131 for 4,096 one-byte reads. The atomics alter generated code
and are a mechanism control, not a performance measurement.

## Behavioral controls

Run [42474764390](https://github.com/mingley/partitionline/actions/runs/42474764390)
formatted the draft and passed focused default/all-feature read-ahead,
buffered fairness, deadline, TCP fragmented-frame and pipeline tests, plus a
TLS 1.2/1.3 loopback test. The new tests dropped a pending second pipelined read
after 128 successful reads, checked the exact retained prefix/body, then
resumed it; they also rejected negative and oversized partially prefetched
lengths before consuming body bytes. Both passed in both profiles.

The run nevertheless **failed overall**: the first mutation script's
multiline match was dedented by YAML and raised `AssertionError` before a
mutant was applied; its second mutant was skipped. The corrected independent
control [42475175918](https://github.com/mingley/partitionline/actions/runs/42475175918)
verified the exact formatted source and passed both positive tests under both
profiles. Clearing retained partial bytes before yielding failed at the
intended cancellation-retention assertion; changing the invalid-prefix arm to
read more bytes failed at the intended invalid-prefix assertion. Both mutants
were restored independently. These controls establish the tested behavior;
they do not establish the absence of every cancellation or protocol defect.

## Synthetic ready-reader measurements

Run [42474130366](https://github.com/mingley/partitionline/actions/runs/42474130366)
used release binaries on one pinned logical CPU of a shared Intel Xeon Platinum
8370C, stable rustc 1.99.1, LLVM 23.1.1. Six rounds alternated baseline and
draft for each of default and all features; the [240 raw round records](synthetic-rounds.csv)
include frame counts, lengths, read limits and elapsed time. The appended
benchmark-only fixture was blob `54172b6741ee2201894539ea58f230ed848942fa`.
The table shows median ns/frame and ratio of medians, not a production workload.

| Case | Default baseline → draft ns/frame | Change | All-feature baseline → draft ns/frame | Change |
| --- | ---: | ---: | ---: | ---: |
| direct 32 bytes | 185.4 → 186.5 | +0.58% | 192.4 → 192.8 | +0.21% |
| buffered 32 bytes | 212.6 → 212.8 | +0.09% | 228.5 → 224.5 | -1.76% |
| direct 128 one-byte reads | 4,011.2 → 4,012.2 | +0.02% | 3,781.3 → 3,775.9 | -0.14% |
| buffered 128 one-byte reads | 4,183.1 → 3,886.1 | -7.10% | 4,255.8 → 3,873.8 | -8.98% |
| direct 64 KiB | 2,398.0 → 2,531.0 | +5.54% | 2,536.7 → 2,501.3 | -1.40% |
| buffered 64 KiB | 2,856.6 → 2,938.1 | +2.86% | 3,009.8 → 2,953.8 | -1.86% |
| direct 256 KiB | 8,857.0 → 9,159.3 | +3.41% | 9,121.1 → 9,077.7 | -0.48% |
| buffered 256 KiB | 12,805.3 → 13,005.5 | +1.56% | 13,151.5 → 12,963.1 | -1.43% |
| direct 4,096 one-byte reads | 133,749.2 → 133,448.2 | -0.23% | 125,951.6 → 125,787.9 | -0.13% |
| buffered 4,096 one-byte reads | 142,058.6 → 130,266.4 | -8.30% | 142,881.0 → 130,266.1 | -8.83% |

All six paired rounds improved for the buffered one-byte cases. The unmodified
direct 64-KiB path varied in both directions across rounds, including a +5.54%
default median-of-rounds change; this and the differing profiles show that
small changes cannot be separated from code generation or shared-host noise
here. The reader is deliberately always ready and does not model a TCP stack.

## TCP and TLS loopback measurements

Run [42474675533](https://github.com/mingley/partitionline/actions/runs/42474675533)
used distinct release binaries on one pinned logical CPU of a shared AMD EPYC
7763, stable rustc 1.99.1/LLVM 23.1.1. The test-only network fixture was blob
`0e95d63eb41a33b0be87dca4ea6ee073b7892db0`; baseline binary SHA-256
`b2be5a217b99883c94001f8bc6733c1f51d33e5e4cd44c394f932c2fccceed95`
and draft binary SHA-256
`8fd0e48e83dbd8a819e276202bd3fd09a82d62a997441e7a90d4a45230e16d2d`.
Six rounds alternated their order.

A single connection sends a wire length prefix and a body, then waits for a
one-byte acknowledgement. The test manually places the prefix in ReadAhead to
exercise that path. TCP_NODELAY is enabled; client and server share one
current-thread executor. Each case uses 8 warmups followed by 128 256-KiB or
256 4-KiB requests. In the 4-KiB case a server-side adapter deliberately caps
each successful read at one byte; it does not describe packet sizes. The
[48 round summaries](network-rounds.csv) and [9,216 individual RTT values](network-rtt-samples.csv)
are retained; timestamps in those files are output-emission times after each
timed interval, not request times. All 48 p50/p95/p99 summaries were recomputed
from their individual values and matched.

Throughput uses the median of six elapsed times. Latencies below are medians
of the six per-round percentile estimates, in microseconds, not pooled or
fleet-level percentiles.

| Case | Baseline → draft MiB/s | p50 baseline → draft µs | p95 baseline → draft µs | p99 baseline → draft µs |
| --- | ---: | ---: | ---: | ---: |
| TCP, 256 KiB | 854.0 → 671.9 (-21.32%) | 279.8 → 362.6 | 400.1 → 397.2 | 439.9 → 411.8 |
| TLS/TCP, 256 KiB | 546.6 → 512.8 (-6.18%) | 484.7 → 485.9 | 517.8 → 691.2 | 526.1 → 718.0 |
| TCP, 4 KiB one-byte adapter | 1.090 → 1.103 (+1.21%) | 3,577.2 → 3,536.6 | 3,622.9 → 3,579.7 | 3,658.7 → 3,605.8 |
| TLS/TCP, 4 KiB one-byte adapter | 12.08 → 12.40 (+2.65%) | 317.1 → 307.9 | 345.9 → 340.0 | 361.2 → 410.1 |

For TCP 256 KiB, draft elapsed time was 21.98% to 29.48% higher in every
paired round. TLS p99 was variable across the small per-round samples. The
cause of the TCP result is not established. The setup does not represent a
complete broker, concurrent-client fairness or a reproducible cross-machine
Kafka comparison. A separate rotated-order, byte-identical-placebo and
additional-test-binary-layout control was queued as
[42475240981](https://github.com/mingley/partitionline/actions/runs/42475240981);
its result is not part of these measurements.
