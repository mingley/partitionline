# Producer `send_all` experiment without an added yield

This isolated candidate reduces repeated leader scans for a warmed batch on one
explicit topic and partition. It does not add the 512-admission yield explored
in the earlier [candidate](../sendall-yield-perf-20261108/README.md). Neither
candidate is applied or qualified as production ready.

The shortcut requires 32–8,191 records with the same explicit nonnegative
partition and topic, a topic name at most 249 bytes, no interceptor or sticky
policy, and a cached fresh accepting worker. Other cases use the original
suffix scan. The fast-worker check and the later cluster lookup are separate:
a cached route does not make metadata and worker replacement atomic. The
shorter scan changes timing under concurrent updates or contention.

## Matched mock measurements

[Run 42471868128](https://github.com/mingley/partitionline/actions/runs/42471868128)
collected 110 interleaved baseline/candidate samples.
[Run 42472036756](https://github.com/mingley/partitionline/actions/runs/42472036756)
collected 12 more at the 8,191/8,192 boundary with 249-byte topics. Each run
used a separate hosted x86_64 AMD EPYC 7763 runner, Rust 1.99.1, LLVM 23.1.1,
and the release profile with thin LTO and one codegen unit. Each exposed four
CPUs; `cpu.max` was unavailable, so quota is unknown. The two binaries in each
run were built in separate target directories and had different hashes.

Each sample warms a route and manually polls public `send_all` on a
current-thread runtime. The mock verifies every value and offset, completion,
byte accounting and error count. The table shows the range of three samples in
milliseconds. [All 122 samples](samples.csv) include first and maximum poll,
completion, cumulative queued count including warmup, timer observations, and
Produce batch sizes.

| Records | Topic bytes | Linger | Baseline max poll | Candidate max poll | Baseline completion | Candidate completion |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 31 | 1 | 0 ms | 0.110–0.156 | 0.110–0.113 | 0.293–0.338 | 0.298–0.306 |
| 32 | 1 | 0 ms | 0.125–0.127 | 0.044–0.045 | 0.285–0.369 | 0.235–0.258 |
| 512 | 1 | 0 ms | 19.421–19.827 | 0.625–0.650 | 20.361–20.691 | 1.448–1.563 |
| 1,024 | 1 | 0 ms | 74.967–75.999 | 1.291–1.362 | 76.482–77.651 | 2.735–2.942 |
| 4,096 | 1 | 0 ms | 1,182.277–1,213.078 | 5.362–5.628 | 1,187.479–1,218.540 | 10.359–10.775 |
| 8,191 | 1 | 0 ms | 4,720.274–4,813.602 | 10.702–10.821 | 4,731.115–4,824.501 | 20.937–21.384 |
| 8,192 | 1 | 0 ms | 4,728.970–4,737.897 | 4,741.685–4,748.679 | 4,741.139–4,750.147 | 4,753.876–4,760.762 |
| 4,096 | 249 | 0 ms | 2,605.142–2,608.288 | 8.187–8.666 | 2,611.735–2,615.383 | 14.146–14.856 |
| 8,191 | 249 | 0 ms | 10,410.893–10,431.239 | 17.089–17.250 | 10,423.424–10,443.754 | 29.402–29.644 |
| 8,192 | 249 | 0 ms | 10,346.868–10,423.567 | 10,286.651–10,311.317 | 10,362.289–10,439.310 | 10,302.118–10,327.484 |
| 4,096 | 1 | 5 ms | 1,181.346–1,187.286 | 5.337–5.392 | 1,186.590–1,192.761 | 10.432–10.494 |
| 8,191 | 1 | 5 ms | 4,724.121–4,735.792 | 10.552–10.763 | 4,734.876–4,746.311 | 20.607–21.265 |

In these trials both arms emitted one batch at each 4,096-, 8,191- and
8,192-record configuration shown. The 8,192-record case falls back and retains
the long poll. The largest observed eligible poll was 17.250 ms for the
249-byte topic: removing the added yield does not impose a 5 ms scheduling
bound. The 5 ms timer was registered before the manual poll, but the runtime
had not driven it when the timer was polled again; a pending result does not
show that its deadline had not elapsed.

The 31/32 boundary was also checked using test-only counters: 31 fallbacks
at 31 and 32 shortcuts at 32. An interceptor-enabled 1,024-record fallback
had one sample per arm (75.574 ms baseline and 75.679 ms candidate max
poll); it is not a distribution. Sticky and interceptor fallbacks were
checked for correctness separately, not benchmarked as a performance matrix.

These samples use the in-repository mock, one warmed partition, and no
contention-heavy or independent-broker throughput workload. They do not
establish Kafka throughput, tail latency under load, or a comparison against
other Kafka implementations. The 8,192 fallback, long eligible poll, metadata
and worker races, and contention remain relevant to production use.

## Correctness controls

Instrumented mock run
[42472219751](https://github.com/mingley/partitionline/actions/runs/42472219751)
passed 13/13 controls in each feature profile. It covered eligibility
boundaries, interceptor/sticky, implicit and mixed routes, invalid partitions,
a gated metadata change and expiry, worker replacement, and a synthetic
full-to-draining connect queue. The gates and counters change scheduling; an
unmodified-source comparison is still needed for performance and cannot cover
every concurrent schedule. Details and exact pins are in the
[route evidence](https://github.com/mingley/partitionline/blob/4882c2a04ba21da0f11471112cbfd8c4a713ed6a/docs/evidence/client/send-all-route-controls-20261108/README.md).

A separate gated mock history
[42472940666](https://github.com/mingley/partitionline/actions/runs/42472940666)
passed three repetitions in each profile. After a public refresh changed the
leader, node 2 received payload IDs 0–63 and returned error code 6; node 1
then accepted the same IDs. Receipts, offsets and mock-log values matched.
The test observed exactly those two 64-record batches in each run. It uses a
forced schedule and an instrumented mock and does not prove behavior for all
metadata races or against a real broker. Its instrumented producer blob was
`9161723433cb7674c1db51e51ecb88852e43c3c5`, trace-mock blob
`a42fabc941f9086d894b9e4831116a8137796389` and test blob
`4365889bde22c6051d02c9123448acb24f096ab1`. The default test binary SHA-256
was `6fc78edeccefd2ebe6c179cf8c8d43b8a58442d2f0d264354fc4ff44b33e5628`;
the all-feature binary was
`1861b23e43ed19aaa4accf9547e8d208495445b4133f2eff55e7aaf4333f2e14`.
The binary hashes were recorded, but the binaries were not retained.

Uninstrumented mock cancellation run
[42472001367](https://github.com/mingley/partitionline/actions/runs/42472001367)
passed 6/6 baseline and 6/6 candidate tests in each feature profile. Under an
unconstrained manual first poll both admitted all 1,025 ready records.
Controlled full-buffer and full-worker-channel cancellations checked accepted
records, receipts, mock log and byte reservations. Caller-owned destructors
could block the cancelling thread at existing waits. Removing the explicit
yield does **not** prove cancellation-boundary equivalence: faster admission
can change when an existing buffer or channel wait becomes pending. See the
[cancellation evidence](https://github.com/mingley/partitionline/blob/87ed8994a802c36497f5587334d36b5262e3cbb3/docs/evidence/perf/sendall-cancel-v5-20261108/README.md).

In a fresh single-broker Apache Kafka 3.9.1 history,
[42471949604](https://github.com/mingley/partitionline/actions/runs/42471949604)
checked all-feature candidate delivery with idempotence off and on. Each mode
verified 22,561 ordered receipt offsets, the exact broker end offset and all
values via Kafka's CLI. The verifier rejected missing, duplicated, altered and
reordered records. The amd64 image tag resolved to digest
`sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed`.
This was one partition on one broker, without failover or a throughput
comparison; the binary hash was recorded but the binary was not retained.

The exact candidate passed Rustfmt and strict all-target/all-feature Clippy in
[42471922968](https://github.com/mingley/partitionline/actions/runs/42471922968).
Its existing mixed-topic redial test failed in both profiles (3 passed, 1
failed), while later `produce_cancel` tests passed 23/23 and
`producer_retained_backing` passed 9 with 1 ignored in each profile.
The same redial assertion failed on the unmodified baseline in all 12 matched
trials; see the
[request timeline](https://github.com/mingley/partitionline/blob/b88b4167f104df585fbef12c9fc7f1f3a36bca64/docs/evidence/client/sendall-redial-timeline-20261108/README.md).
This existing failure remains open.

## Source pins

- Baseline commit `47df27f9f3e04fc524849f3a9753c0b0469f8947`,
  producer blob `1e7733c136e54b532cdc8bddabd1e0c828e50ed2`,
  SHA-256 `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`.
- Candidate commit `a781fbf47e724d04d37426e2b1c6b13dc2692003`,
  producer blob `650e34cca0cfa0bee9825ffc7d7da86514e28a31`,
  SHA-256 `e074dfdc9d91a64cf25102a747e82af4aaf9be4baf4280b148f42b49280464b3`.
- Identical diagnostic SHA-256
  `33c8a8d94d48e803d856f3554f0771e4b7233cd9e398b709b45dc9ea3604e96b`,
  mock fixture SHA-256
  `b3761d62c090d87251c1825e64ec468c5e092d22ca27fd4a690b46b7631fa5b1`,
  and lockfile SHA-256
  `bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
- Baseline benchmark binary SHA-256
  `7cd87185768bf7917154a563e49f5fc5a7df6e849f5a085547b53dda39ccc62e`;
  candidate `a8b671ebb5d13d1f2366f4c18f97728280eac2b9e21fd988ff2bd1d00c7bce2e`.
  Both runs reported these hashes; the binaries were not retained. Raw logs
  were retained for [the 110 samples](https://github.com/mingley/partitionline/actions/runs/42471868128/artifacts/13614440876)
  and [the long-topic boundary](https://github.com/mingley/partitionline/actions/runs/42472036756/artifacts/13614575461).
