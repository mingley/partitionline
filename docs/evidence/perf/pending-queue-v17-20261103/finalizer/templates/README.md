# Pending queue microbenchmark

This is an uncompiled source prototype for a paired Linux x86_64 comparison of
the v17-qualified fixed source and the queue behavior from v10. The candidate
arm keeps its historical `v16` label because v17's fixed catalog is byte-identical. Run it only after the
complete v17 candidate qualification succeeds in the same GitHub Actions job.
The prior failed candidate runs do not satisfy that gate; this rebind has no native result.

The runner copies the qualified fixed source twice. In the control copy it
reverses the v16 deadline test fixture copied into v17, the v14 test import, and the v2
queue patch, in that order. The first two changes are test-only. It
requires the exact v10 producer and prepare-test hashes and checks that only
those two source files differ. It verifies the forward and reverse roundtrip
and records the complete input inventories. Both copies receive the same
release-only benchmark hook and compile with the same Cargo lockfile, feature,
release profile and stable compiler. The fixture reuses each arm's actual
worker initializer, with the same explicit borrow of the fixture-owned shared
state in both arms. The runner checks that the initializers differ only in the
new queue fields. `cfg(test)` is not enabled, so the test-only atomic
witnesses in the qualified source do not run in the measurement.

Each sample uses a current-thread Tokio runtime, a loopback socket fixture and
one pinned allowed CPU. The socket does not exchange Kafka requests. The
bounded cases are:

- `not_expired`: repeat 128 expiration probes on a non-sticky queue with no
  expired records, exposing the full-scan and cached-minimum paths.
- `ordered`: drain queues with increasing deadlines in batches of one or 64,
  exposing both deadline rescans and remaining vector-prefix shifts.
- `descending`: drain strictly decreasing deadlines, keeping the minimum at
  the end so neither arm repeatedly rescans it; vector shifts remain.
- `zigzag`: alternate high and increasing low deadlines to exercise the
  nonmonotonic fallback.
- `admit_ordered` and `admit_zigzag`: time individual `Worker::push_pending`
  calls with already prepared records. They do not measure the main worker's
  `recv_many` and `account_pending_from` path or sticky partitioning.

The probe uses 512, 4,096 and 8,192 records; the other cases use 512 and 4,096.
The drain timing includes moving returned records into a preallocated result
vector and returning the emptied batch allocation to the worker. Record
construction, socket setup and synthetic acknowledgement happen outside the
timed section. Both arms use identical operations and shape parameters. The
probe and drain cases do not measure admission cost; admission has its own
cases. No claim follows about end-to-end producer throughput, broker delivery,
durability, memory limits under load, or comparative ranking.

Every accepted record has the deterministic integer identity `0..count-1`
encoded as an eight-byte key. After timing, the harness checks the returned
order and identity set and uses the actual worker completion path with a
synthetic acknowledgement. It checks each oneshot result, partition, offset
and buffered byte balance. Missing, duplicate, invalid, out-of-order, wrong,
failed, closed and pending identities are reported in the raw sample. A bad
sample invalidates its pair and stops further sampling after the paired arm;
its raw output, process exit, cleanup receipt and state remain on disk. This
checks in-memory ownership and synthetic completion, not delivery to Kafka.

Each case has one warmup pair and ten measured pairs. Measured order has five
of each arm ordering, shuffled by a recorded fixed seed. The runner retains
raw stdout/stderr and a JSON line per process, not just aggregate statistics.
It records compiler and Cargo versions, CPU metadata, affinity, governor,
source/patch/runner hashes, binary hashes, command exits, disk and cleanup
receipts. It does not compute a speed claim. Hosted runners can be shared and
frequency or scheduler noise can still bias observations.

## Checks and use

The Python static controls run without Cargo. Set all five source paths and the
checkout path to check patch reconstruction, benchmark-hook parity, helper
pins and qualification controls; checks without their inputs are skipped. For example, after v17 qualification:

```sh
export PL_BENCH_CHECKOUT_PATH="$GITHUB_WORKSPACE"
export PL_BENCH_DEADLINE_PATH="$GITHUB_WORKSPACE/docs/evidence/client/candidate-20261030/v17/deadline-fixture.patch"
export PL_BENCH_PRODUCER_PATH="$RUNNER_TEMP/candidate-qualification/source/src/producer.rs"
export PL_BENCH_PREPARE_TESTS_PATH="$RUNNER_TEMP/candidate-qualification/source/src/producer/prepare_tests.rs"
export PL_BENCH_CARGO_PATH="$RUNNER_TEMP/candidate-qualification/source/Cargo.toml"
export PL_BENCH_IMPORT_PATH="$GITHUB_WORKSPACE/docs/evidence/client/candidate-20261030/v14/pending-work-import.patch"
python3 -B docs/evidence/perf/pending-queue-v17-rebind-20261103/test_plan.py
python3 -B docs/evidence/perf/pending-queue-v17-rebind-20261103/bind_qualification.py \
  --qualification "$RUNNER_TEMP/candidate-qualification" \
  --checkout "$GITHUB_WORKSPACE" \
  --output "$RUNNER_TEMP/pending-queue-qualification.json"
python3 -B docs/evidence/perf/pending-queue-v17-rebind-20261103/run.py \
  --qualification "$RUNNER_TEMP/candidate-qualification" \
  --checkout "$GITHUB_WORKSPACE" \
  --witness "$RUNNER_TEMP/pending-queue-qualification.json" \
  --output "$RUNNER_TEMP/pending-queue-bench"
```

The example assumes this inactive bundle is extracted under `docs/evidence/perf/pending-queue-v17-rebind-20261103`.

The illustrative workflow creates a witness immediately after successful v17
qualification in the same job. It requires the exact format and clippy gates,
and every pinned target in both default and all-features profiles, in addition
to the existing 166 library and 17 integration control checks. The runner checks the witness against the
current run, attempt, job, checkout SHA, driver and qualification report
hashes, the six reviewed helper file pins, and the v17 catalog and patches.
The guard checks file bytes; it does not verify dependencies or prove live
execution. Replayed witnesses from an
older run or attempt fail. This relies on the workflow's same-job ordering and
is not an independently signed attestation of an arbitrary copied result.
The runner requires at least 1.5 GiB free before and during builds and samples.
It monitors and cleans up release-build and sample process groups; short setup
and version commands use ordinary subprocesses. Build logs and partial sample
output are retained on failure. `workflow-fragment.yml` is illustrative and is
not an active workflow.

The Rust fixture and the complete workflow have not been compiled or run.
A successful static check does not establish that they compile, that the
qualification passes, or that the optimization is faster. Review the exact
source and run the first build before relying on this harness.
