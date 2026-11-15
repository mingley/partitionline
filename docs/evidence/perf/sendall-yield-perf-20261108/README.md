# Producer `send_all` homogeneous-route experiment

This is evidence for an isolated candidate, not a change on main or a general
Kafka performance claim. For fewer than 8,192 records, the candidate detects a
single explicit topic and partition, then avoids repeatedly scanning the same
remaining suffix when a fresh worker is cached. It adds a yield after 512
admissions. Interceptors, sticky routing and other inputs use the old scan.

The new yield is material: in a [controlled cancellation history](../sendall-cancel-yield-20261108/README.md) it left 513
of 1,025 caller-owned inputs unaccepted. A deliberately blocking destructor for
one of those inputs delayed the cancelling thread and a registered timer for
about 3 seconds. A separate no-added-yield candidate is being evaluated; the
results below apply only to the yielding candidate.

## Matched mock measurements

Run [42470976883](https://github.com/mingley/partitionline/actions/runs/42470976883)
used one GitHub-hosted x86_64 AMD EPYC 7763 runner, Rust 1.99.1, LLVM 23.1.1,
and the repository release profile (thin LTO, one codegen unit). Four CPUs were
reported; `cpu.max` was unavailable, so the quota is unknown. It alternated
baseline/candidate order, built into separate target directories, and used
three samples per ordinary configuration. One extra interceptor-enabled
fallback sample per arm was collected. In all 110 samples, the mock's ordered
record values, returned offsets, byte accounting and error count passed. The
[110 individual samples](samples.csv) retain nanosecond values and batch sizes;
queued counts include the warmup record.

The benchmark warms a one-partition route, uses a current-thread runtime, no
interceptor and explicit partition 0, then measures a manually polled public
`send_all`. Completion includes the mock acknowledgments. The 5 ms timer is
registered before the poll, but it is polled again before the runtime drives
timers; its remaining pending does not mean that 5 ms had not elapsed. The
maximum poll and completion values below are the ranges of three samples in
milliseconds.

| Records | Topic bytes | Linger | Baseline max poll | Candidate max poll | Baseline completion | Candidate completion |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 1 | 0 ms | 0.125–0.138 | 0.044–0.046 | 0.323–0.331 | 0.232–0.252 |
| 512 | 1 | 0 ms | 19.521–21.201 | 0.634–0.651 | 20.431–22.007 | 1.423–1.581 |
| 1,024 | 1 | 0 ms | 75.645–83.174 | 0.725–0.736 | 77.230–84.757 | 2.816–3.193 |
| 4,096 | 1 | 0 ms | 1,183.420–1,189.734 | 1.060–1.098 | 1,189.045–1,195.358 | 9.405–9.495 |
| 8,191 | 1 | 0 ms | 4,722.511–4,824.742 | 1.463–1.507 | 4,733.648–4,835.571 | 16.038–16.323 |
| 8,192 | 1 | 0 ms | 4,735.345–4,809.508 | 4,735.691–4,740.579 | 4,747.323–4,821.943 | 4,747.893–4,752.502 |
| 4,096 | 249 | 0 ms | 2,605.597–2,609.694 | 1.498–1.534 | 2,611.968–2,616.394 | 13.734–13.977 |
| 4,096 | 1 | 5 ms | 1,184.394–1,199.829 | 1.021–1.075 | 1,189.742–1,205.272 | 9.555–9.790 |
| 8,191 | 1 | 5 ms | 4,727.108–4,743.886 | 1.463–1.581 | 4,738.175–4,754.685 | 23.519–25.220 |

The candidate changes batching. At 4,096 records with zero linger, baseline
sent one batch of 4,096 and candidate sent batches of 1,024, 2,048 and 1,024
in every sample. With 5 ms linger both sent one 4,096-record batch. At 8,191
and zero linger, baseline sent one batch; candidate sent
`[1024, 2048, 2048, 2048, 1023]`. With 5 ms linger the candidate sent
`[5632, 2559]` twice and `[5120, 3071]` once; baseline sent one batch.
At 8,192 the candidate takes the old path and the long poll remains.

These are local mock observations, not socket cost or throughput against
Kafka or another implementation. Other sizes, concurrent workloads, broker
latency, hardware and scheduling can produce different results.

Source pins and provenance:

- Baseline commit `47df27f9f3e04fc524849f3a9753c0b0469f8947`,
  producer blob `1e7733c136e54b532cdc8bddabd1e0c828e50ed2`,
  SHA-256 `3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f`.
- Candidate commit `3fd30f13c07056dc0ac3d937b3f370d92880bd98`,
  producer blob `d634fa4752e33d77e042404f199f31ee2bcb1c48`,
  SHA-256 `10c84895af80e31c3c9e8a18bd84480caf248d228adece495c1539cdd4c902d9`.
- Identical diagnostic SHA-256
  `33c8a8d94d48e803d856f3554f0771e4b7233cd9e398b709b45dc9ea3604e96b`,
  mock fixture SHA-256
  `b3761d62c090d87251c1825e64ec468c5e092d22ca27fd4a690b46b7631fa5b1`,
  and Cargo.lock SHA-256
  `bdc37c48feb0d7a17a1b08e70c5301ec30b9a90e64c93adaf28aef6b6f9f46bc`.
- Baseline binary SHA-256
  `7cd87185768bf7917154a563e49f5fc5a7df6e849f5a085547b53dda39ccc62e`;
  candidate `5abe3ff9199c608b4878380fade5b3f578a52a76ca48bcd1b2ae0ba669db39b7`.
  The binaries themselves were not retained. The
  [raw trial logs](https://github.com/mingley/partitionline/actions/runs/42470976883/artifacts/13613623121)
  were retained.

## Correctness scope and unresolved checks

In [42471564142](https://github.com/mingley/partitionline/actions/runs/42471564142),
a fresh single-broker Apache Kafka 3.9.1 image resolved to
`sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed`
on amd64. The all-feature yielding-candidate binary exercised explicit
partition 0 at sizes 32, 512, 513, 1,024, 4,096, 8,191 and 8,192. For each
of idempotence off and on it verified 22,561 ordered public receipt offsets
including warmup, exact broker end offset, and all values through Kafka's CLI.
The independent verifier rejected missing, duplicated, changed and reordered
values. The image tag was resolved and logged; the binary hash was logged but
the binary was not retained. This does not test multi-broker failover or
concurrent metadata changes.

The matched cancellation histories are described in the linked
[control run](https://github.com/mingley/partitionline/actions/runs/42471492541):
both default and all-feature baseline and candidate arms passed 6/6 controls.
They deliberately use an unconstrained manual first poll and a mock, so they
do not establish ordinary executor timing or a nonblocking destructor.

Run [42471313301](https://github.com/mingley/partitionline/actions/runs/42471313301)
passed Rustfmt and strict all-target/all-feature Clippy for the yielding
candidate. Its default and all-feature producer jobs both failed the existing
mixed-topic `send_all_redials_leaders_while_refreshing_metadata` timing
assertion at 759.073 ms and 755.417 ms respectively (limit 450 ms). Each had
3 passed and 1 failed; later cancellation and retained-backing suites were
not run in those jobs. The exact failed jobs were
[default](https://github.com/mingley/partitionline/actions/runs/42471313301/job/128702472985)
and [all features](https://github.com/mingley/partitionline/actions/runs/42471313301/job/128702472573).
Its two mixed records do not meet the shortcut's source-level eligibility.

In matched run [42471756660](https://github.com/mingley/partitionline/actions/runs/42471756660),
all six trials of the four-test group failed the same redial assertion on each
source in each profile: 12 failures per source, with three other tests passing
per trial. Baseline/candidate elapsed ranges were 756.787–757.823 /
756.473–757.957 ms with default features and 756.921–758.446 /
756.979–760.700 ms with all features. The workflow successfully collected
these failures; it did not make the test pass. The cause remains unproven.
The yielding candidate then passed `produce_cancel` (23 passed, 0 ignored)
and `producer_retained_backing` (9 passed, 1 ignored) in both profiles.

The distinct no-added-yield source also failed this assertion in its own
[default](https://github.com/mingley/partitionline/actions/runs/42471922968/job/128704453918)
and [all-feature](https://github.com/mingley/partitionline/actions/runs/42471922968/job/128704453922)
jobs at 758.844 and 757.571 ms. Its later cancellation and retained-backing
suites had the same 23 passed and 9 passed/1 ignored counts in each profile.
Both candidates remain unapplied; their remaining checks are separate.

An instrumented route-control run
[42471551212](https://github.com/mingley/partitionline/actions/runs/42471551212)
passed 8 of 9 tests in each profile. The 249-byte topic case failed during
warmup because the fixture had not created the topic; no conclusion about the
candidate follows from that failure. The corrected run [42471911610](https://github.com/mingley/partitionline/actions/runs/42471911610)
passed all 10 controls in both profiles. It includes a registered 249-byte
topic, rejection of a 250-byte name, a gated metadata-age expiry, the 8,191
and 8,192 selection boundaries, worker replacement, and synthetic queue and
cancellation schedules. The test-only instrumented producer blob was
`da6bde348a24e0d28879fc2ad5d2b8b3fa99af74` and its control blob
`234eccde608c594cca82080f86940eae1a66c96b`. These are mock controls with
test-only gates and counters; they do not establish atomicity of cached routes
or all metadata/retirement races.
