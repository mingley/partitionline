# Tokio runtime comparison

KL09-61 is a local, unsigned comparison on October 7, 2026. Suite HOLD remains
active. It does not qualify production readiness or a performance ranking.

Five configurations ran five interleaved repetitions each: current-thread and
multithread with 1, 2, 4 and 5 background workers. The client was pinned to CPUs
2 and 4; the owned Kafka 4.3.1 broker used CPUs 0 and 1. Five workers is the host's
observed available CPU count, not the client's affinity width. Four and five
workers therefore oversubscribe the two client CPUs. CPU frequency was not
controlled. The locked build used latest-stable Rust 1.99.0 and Tokio 1.53.2.

## Results

These are medians of five primary repetitions. Bulk columns are records/s.
The last column is scheduled-arrival-to-observed-ack p99 at 1,351 offers/s.

| Runtime | Null produce | Null fetch | Native bulk | Low-load p99, µs |
| --- | ---: | ---: | ---: | ---: |
| current-thread | 75,802 | 106,803 | 245,443 | 3,784 |
| multithread, 1 worker | 76,962 | 109,706 | 214,925 | 3,742 |
| multithread, 2 workers | 75,290 | 112,216 | 216,801 | 2,806 |
| multithread, 4 workers | 67,813 | 103,637 | 233,172 | 3,216 |
| multithread, 5 workers | 78,467 | 107,856 | 214,721 | 3,146 |

Current-thread native bulk used a median 4,120 client CPU ns/record; multithread
used 8,514–9,254. Those native observations cover the whole workload, including
setup, warmup, diagnostic output and close. Null-broker resources cover timed
work. The process-wide allocation census can affect contention; these are
results for the instrumented drivers.

Matched-repetition throughput deltas relative to current-thread were −9.33%
for one worker (95% bootstrap interval −33.14% to −6.63%), −6.62% for two
(−17.45% to −4.82%), −4.55% for four (−25.91% to +0.90%) and −13.57% for five
(−17.52% to −3.69%). Each interval resamples the five observed paired percent
deltas 20,000 times, using seed 961 plus the cell index. Per-arm intervals,
resource measurements, outcomes and all observed values are in
[analysis-01.json](study/analysis-01.json).

The open-loop offered rates were fixed at 1,351, 6,757 and 10,811 records/s for
every runtime. These are 10%, 50% and 80% of a separate current-thread sequential
calibration median of 13,513.5 records/s, not each runtime's saturation rate.
All low-load runs completed without rejection. At medium load, current-thread
repetition 3 rejected 1,138 offers and two-worker repetition 5 rejected 11. Every
high-load run, including reruns, rejected offers. All **32 capacity failures**
remain failed results. Their acknowledged-record latency cannot qualify a
winning configuration. Unknown and timeout outcomes were zero.

Fresh current-thread cohorts repeated every cell five times. Median throughput
changed by +0.82% for null produce, +0.87% for null fetch and +2.75% for native
bulk. Low-load p99 changed by −31.45%, and medium-load p99 by −30.75%. That
variation and the failed profiles prevent a general latency recommendation.
The guide recommends measuring the application's own scheduling and admission
behavior. No client defaults were changed.

## Qualification and source

There are 60 null-broker results, 120 native results and five separate calibration
runs. Each native bulk run acknowledged eight million timed records after
10,000 warmup records. A genuine Apache Java client independently checked all
240,300,000 bulk records, including keys and values. Open-loop runs offered
20,000 timed records and checked every acknowledged payload and partition offset.
Their fixed payloads have no unique producer record IDs; no stronger identity
claim is made. Bulk latency samples bound admission calls to final flush and
are not individual acknowledgment times.

The executed driver source is commit
`17a6344b019e65ad209800e1f93481f7bf50ad36`. Its 181 checked-out files, release
executables, executed controllers, helper classes, plans, source guards, raw
observations, logs and wait receipts are retained. The original examples and
client source were unchanged. The harness passed 26 tests, formatting and strict
all-target/all-feature Clippy. Failed build and test attempts are also retained.

The original native formatter emitted descriptive broker mode, incomplete
histogram bounds and incomplete attempt metadata. The existing report CLI
accepted those results, but the full JSON schema did not. A separate formatter
at `63a89f4c3475a75bf57557515b7ffab3202eebea` produces
[canonical native views](study/canonical-native-01/manifest.json). It preserves
the original measurements and outcomes and binds both representations by hash;
it does not repeat timing or replace the originals. All 120 views pass the full
schema and report CLI. Every null-broker result and original native result also
has a waited report CLI exit of zero. Two retained failed-test result files pass
that CLI as well: **302 positive CLI invocations** in total. Full-schema
qualification is claimed only for the 120 canonical native views.

Actual changed-result controls rejected 480 original native mutations and 480
canonical mutations through semantic checks, runtime checks or schema validation.
These controls invoke the validation functions; they are not 960 subprocess CLI
runs. Final verification checked 649 waited process receipts, result hashes,
source hashes and closure. Native client exit 1 is retained for all 32 capacity
failures. The broker's shutdown exit 143 was intentional; its supervisor joined,
its process group was empty and both listener ports were reusable.

Producer close returned while three cancelled Tokio tasks were still observable
on current-thread execution. This observation does not prove a permanent leak.
The harness uses a bounded two-second cancellation barrier outside timing.
Production shutdown completion remains the separate pending **KL02-12** card;
its actual failed test, source, executables and results are in
[the observation directory](study/producer-close-observation-01/).

## Archive and reproduction

[archive-inventory.json](archive-inventory.json) maps every retained file's
original path, byte count and SHA-256 to its published representation. Large
files and executables use deterministic gzip compression; smaller files retain
their original bytes. Python caches and Git publication scratch indexes are
excluded. The checksum-pinned upstream Kafka distribution and SDK archives are
external inputs, not copied into this dossier.

From the repository root, verify stored and reconstructed bytes with:

```bash
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/perf/runtime-flavors
```

`SHA256SUMS` also covers the published files. Compressed files can be read with
`gzip -dc`; paths in their JSON retain the original workspace provenance. They
do not imply that a clone contains those original absolute paths. The source
snapshot and [measurement instructions](../../../../benchmarks/runtime/RUNTIME-FLAVORS.md)
describe a fresh run. Allocation, RSS, warmup and shutdown scopes are recorded
with the results. Independent reproduction on controlled hardware remains open.
