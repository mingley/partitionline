# Hosted x86 producer drain diagnostic

This is an unexecuted, source-only experiment. The candidate is not qualified
for integration, and no speedup is established. Run it only after source review.
The result is a validating null-broker client-ceiling diagnostic: the broker
does not establish Kafka storage, replication or independent record readback.
The runner is shared, sample floors are exploratory, and the experiment cannot
establish production readiness, a release gate, or a fastest-client claim.

The capsule expects to live at
`docs/evidence/perf/producer-full-drain-20261103/hosted/`, and its workflow at
`.github/workflows/producer-full-drain.yml`. The workflow triggers on `main`
pushes changing either path, or by manual dispatch. It checks out only the
capsule and fetches the pinned subject commit. The driver archives only the
root client sources, tests, examples and null-broker sources into two isolated
source trees, rather than copying the evidence tree.

The base is `45cbff24de76490594970fc962cd04e7be9fafb1`. The frozen candidate
patch SHA-256 is `aab17e656ec58d263f084bf60055da99e35e4c506317ad8b5042eded1a80491a`;
the Worker-control patch is
`cc21e5d9dde0a076cfbb1f092a4f556ea79eecdd91764affa61f9370d8b8884f`.
The driver verifies the patches and base and candidate producer source hashes.
The baseline gets the same added tests, with the original implementation and
allocation-pointer expectations; source diffs are retained for review.

Both arms run the following commands with the same stable Rust and runner.
The producer suite is serial to bound host load; individual test thresholds and
assertions are unchanged. All four commands run in each arm and every failure
is kept. The driver checks that all four helper tests and the new Worker test
actually report success. No timing runs start unless every command passes in
both arms.

```sh
cargo +stable fmt --all -- --check
cargo +stable clippy --locked --all-targets --all-features -- -D warnings
cargo +stable test --locked --lib producer::tests:: -- --test-threads=1
cargo +stable test --locked --all-features --lib producer::tests:: -- --test-threads=1
```

The new Worker test calls the real `Worker::drain_pending` on partial and full
queues and checks order, estimates, minimum deadline, sender lifetime and
reservation release. It constructs the worker directly and does not test
producer admission, concurrent workers, cooperative cancellation, actual broker
delivery or runtime shutdown. The separate existing cancellation and release
requirements remain open. The new test and the workflow have not compiled or
run; a green static check is not evidence that they will compile or pass.

After the gate, both examples receive the same retained phase-boundary
transformation. Immediately after each flush, it snapshots accepted and
acknowledged counts and producer errors before stopping the timer; a mismatch
fails the attempt even if `close` later catches up. The harness validates both
phase snapshots and the final post-close counters. Each source tree then builds
this timing binary:

```sh
cargo +stable build --locked --release --example bench_produce
```

The retained copy is timed with `/usr/bin/time -v -o client.time <binary>`.
Each measurement starts a fresh validating null broker built once from the
pinned `nb-serve.rs`, waits for its ephemeral loopback port, runs the producer,
shuts down the broker and retains its JSON artifact. The broker and client are
ordinary descendants of a single owned trial session, with its cleanup receipt
retained for normal, failed and interrupted commands. Common settings are 100
byte seeded values, ID keys, acks 1, no idempotence, no compression, one
connection, a 1,048,576 byte batch limit and 10,000 warmup records:

| Case | Partitions | Batch record cap | Linger | In flight | Timed records |
|---|---:|---:|---:|---:|---:|
| `one_record` | 1 | 1 | 0 ms | 1 | 20,000 |
| `bulk` | 6 | 32,768 | 5 ms | 5 | 1,000,000 |

Five seeded, randomized A/B pairs run for each case. Exact environment and
command are saved per attempt. A producer failure, mismatched acknowledgement
counts, broker framing/CRC/count/sequence errors, leader mismatches, missing
broker artifacts, missing phase snapshots, mismatched effective settings, or
per-partition offsets inconsistent with the intended round-robin workload mark
that attempt failed; all attempts and failures remain in the output. Counts below the repository's
qualification floors are explicitly exploratory, not altered gate thresholds.

After timing, separately instrumented builds run one census per arm and case;
the counters reset at the warmup/measurement boundary, are read before close,
and record nonempty full drains, equal-capacity eligibility, total and eligible records, and size bins.
Their timings are not used in the A/B comparison. The default partitioner on
these configured examples uses the non-sticky worker path. A zero eligibility
result is valid; zero calls indicates that the intended path was not exercised.

The runner requires GitHub Actions on native x86_64, validates the exact
workspace and owned work/output paths, and samples workspace, temporary, Cargo
and rustup mounts during commands. It stops at 2 GiB free to leave 512 MiB of
headroom above the 1.5-GiB floor; a large burst between samples can still cross
that floor. It reserves space for source archives before extracting them. A
SHA-256-verified command-ownership helper controls every managed session and
records cleanup receipts. A deadline 150 minutes after workflow start leaves
30 minutes before the 180-minute job timeout for artifact upload. Archive,
build and gate logs, raw
client and broker output, drain census, source diffs, workload settings,
binary hashes, runner metadata and randomized order are retained by the
workflow even when a gate fails. The measured executables are not retained;
their hashes and complete source/command inputs are. GitHub cancellation or a
hard runner failure can still prevent upload.

Before trusting results, independently review the patches and harness, run the
hosted job, inspect its gate and attempt logs, and compare the paired raw
results. A matched real Kafka experiment with independent payload verification,
latency distributions and named peers remains separate work.
