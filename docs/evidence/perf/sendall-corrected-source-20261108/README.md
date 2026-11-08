# Corrected-source `send_all` mock measurements

The candidate remains unapplied: the exact combined behavioral run
[42474072730](https://github.com/mingley/partitionline/actions/runs/42474072730)
failed 17 of 45 `mock_produce` tests in each profile. The
[combined qualification](../../client/send-all-combined-qualification-20261108/README.md)
retains those failures and the passing narrower controls.

This report records the separate corrected-source benchmark
[42473834473](https://github.com/mingley/partitionline/actions/runs/42473834473),
which passed all 122 mock samples. The candidate producer blob was
`31bdaf849244026b3d64334b1429f676d8d4e755`. It differs from the source in
the [earlier measurements](../sendall-no-yield-20261108/README.md) by a moved
doc comment. The runs are separately pinned even though their candidate
binary hashes matched.

## Results

The run used one GitHub-hosted x86_64 Intel Xeon Platinum 8573C runner, Rust
1.99.1, LLVM 23.1.1, and the release profile with thin LTO and one codegen
unit. Four CPUs were reported; `cpu.max` was unavailable, so the quota is
unknown. Baseline and candidate were built in separate target directories and
run in alternating order. A one-partition route was warmed before each sample.
The public `send_all` future was manually polled on a current-thread runtime.
Every sample checked ordered receipt offsets, mock-log values and accounting.

The table shows the range of three samples in milliseconds. All
[122 individual samples](samples.csv) retain first and maximum poll,
completion time, queued count including warmup, timer observations and Produce
batch sizes.

| Records | Topic bytes | Linger | Baseline max poll | Candidate max poll | Baseline completion | Candidate completion |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 31 | 1 | 0 ms | 0.081–0.085 | 0.084–0.089 | 0.166–0.177 | 0.177–0.202 |
| 32 | 1 | 0 ms | 0.090–0.125 | 0.035–0.047 | 0.178–0.250 | 0.146–0.172 |
| 512 | 1 | 0 ms | 13.935–15.381 | 0.475–0.613 | 14.590–16.043 | 1.054–1.271 |
| 1,024 | 1 | 0 ms | 55.266–59.382 | 0.994–1.113 | 56.572–60.862 | 2.152–2.431 |
| 4,096 | 1 | 0 ms | 862.046–873.526 | 4.562–4.775 | 867.146–878.458 | 8.508–9.476 |
| 8,191 | 1 | 0 ms | 3,396.185–3,464.093 | 8.250–8.386 | 3,405.580–3,473.603 | 16.413–17.398 |
| 8,192 | 1 | 0 ms | 3,414.127–3,457.738 | 3,394.056–3,412.143 | 3,424.844–3,468.690 | 3,404.819–3,422.833 |
| 4,096 | 249 | 0 ms | 2,172.317–2,194.128 | 7.084–7.645 | 2,178.239–2,200.406 | 11.818–13.335 |
| 8,191 | 249 | 0 ms | 8,672.329–8,753.822 | 14.259–15.387 | 8,684.817–8,766.438 | 25.869–27.176 |
| 8,192 | 249 | 0 ms | 8,792.382–8,918.519 | 8,640.268–8,788.603 | 8,807.251–8,933.397 | 8,654.697–8,803.772 |
| 4,096 | 1 | 5 ms | 846.968–862.677 | 4.240–4.456 | 851.606–867.266 | 8.355–8.830 |
| 8,191 | 1 | 5 ms | 3,409.131–3,455.038 | 8.417–9.412 | 3,419.293–3,464.568 | 17.517–19.181 |

For the 4,096-, 8,191- and 8,192-record rows, each arm emitted one Produce
batch in every sample. The eligible 249-byte-topic candidate poll reached
15.387 ms, and the 8,192-record case used the fallback with multi-second
polls. There is no demonstrated 5 ms scheduling bound. The timer was
registered before the manual poll but polled again before the runtime drove
timers, so a pending timer does not establish that its deadline had not
passed.

The shortcut is configured to start at 32 records. This uninstrumented run
does not itself prove which branch was selected. Instrumented controls for
the earlier source checked the 31/32 boundary, sticky and interceptor
fallbacks, and selected metadata and worker-change schedules; see the
[route evidence](../../client/send-all-route-controls-20261108/README.md).
The interceptor-enabled 1,024-record fallback here had one sample per arm:
56.294 ms baseline and 55.539 ms candidate maximum poll.

These are local mock observations, not a throughput comparison with Kafka or
another implementation. No contention-heavy workload, independent performance
reproduction or general fastest claim follows from them. The fast-cache check
and cluster lookup occur at different times; metadata changes, worker
replacement and full or draining connect queues can alter the schedule.
Faster admission can also change when existing buffer or channel waits become
pending, so cancellation boundaries are not proven equivalent.

## Source pins

- Baseline commit `47df27f9f3e04fc524849f3a9753c0b0469f8947`,
  producer blob `1e7733c136e54b532cdc8bddabd1e0c828e50ed2`, SHA-256
  `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`.
- Candidate commit `1af801af08e89931f53b811da49be896783e1ec4`,
  producer blob `31bdaf849244026b3d64334b1429f676d8d4e755`, SHA-256
  `9f66f2a399a3cc14d40f7a69de7ffb08ee63f91cc3f48cab1a089e0750d2d37c`.
- Identical diagnostic SHA-256
  `33c8a8d94d48e803d856f3554f0771e4b7233cd9e398b709b45dc9ea3604e96b`,
  mock fixture SHA-256
  `b3761d62c090d87251c1825e64ec468c5e092d22ca27fd4a690b46b7631fa5b1`,
  and lockfile SHA-256
  `bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
- Baseline binary SHA-256
  `7cd87185768bf7917154a563e49f5fc5a7df6e849f5a085547b53dda39ccc62e`;
  candidate
  `a8b671ebb5d13d1f2366f4c18f97728280eac2b9e21fd988ff2bd1d00c7bce2e`.
  The binaries were not retained. The
  [raw run logs](https://github.com/mingley/partitionline/actions/runs/42473834473/artifacts/13614673433)
  were retained.

A separate corrected-source Apache Kafka history
[42473919585](https://github.com/mingley/partitionline/actions/runs/42473919585)
checked 22,561 ordered receipts and independent CLI values per idempotence
mode on one broker. It was a correctness check, not a throughput comparison.
