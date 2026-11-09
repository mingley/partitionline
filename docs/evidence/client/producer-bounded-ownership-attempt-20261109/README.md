# Bounded producer ownership experiment

This is an isolated, incomplete implementation and is not applied to main.
It does not qualify producer delivery, Kafka interoperability, production
readiness or performance. The baseline is main
[`731eb97a08445a18da0b043986ae192397ba176a`](https://github.com/mingley/partitionline/tree/731eb97a08445a18da0b043986ae192397ba176a),
producer blob `426dbdb5c5e27fdf655930a76682c5c18fb07e89`.

The experiment adds an opt-in `Producer::new_bounded` record-slot limit.
An admitted actual record and its receipt stay in an owner-held slot while a
non-cloneable lease moves through the existing worker, grouping, in-flight,
retry and completion paths. A dropped committed lease marks the slot for a
supervisor and marks the producer closed; it does not use a separate cleanup queue.
The record guard is dropped before an interceptor callback. Bounded close
seals admission before its flush and defers interceptor close until owned
callbacks and payload destruction finish. A close timeout can return before
that deferred interceptor close. The constructor coalesces interceptor close
across close calls. The legacy constructor has no new record-slot limit,
though the experiment changes the common `Pending` representation; its
performance has not been measured.

These mechanisms do not yet form a safe producer implementation. In particular,
existing retries still move records independently; there is no authoritative
sealed batch journal, partition fence, sequence rollover repair or
identity/write fence. Closing admission after an orphan does not prove that an
already offered write cannot start or finish. The supervisor can invoke a
blocking callback synchronously; bounded-mode callback and interceptor-close
panics are caught, and the intended error-reporting and runtime-shutdown
contract are not established. Do not apply this experiment without resolving the
remaining behavior below.

## Exact sources and ordinary controls

The latest source checkpoint is
[`fb769da637f939eb86317d6c16ac6a3c1843ddf9`](https://github.com/mingley/partitionline/commit/fb769da637f939eb86317d6c16ac6a3c1843ddf9),
with `src/producer.rs` blob `f2a0384c0da06860f24d26c78745c9100757e513`
and `src/producer/owned.rs` blob
`ea0ca2f02a786455b36b64e15a539e2d9ad947be`. The latter contains a
`#[cfg(test)]` destructor control. The [qualification branch](https://github.com/mingley/partitionline/tree/qualification/producer-owned-lease-20261109)
also contains the public test fixtures and workflow.

[Run 42687371308](https://github.com/mingley/partitionline/actions/runs/42687371308)
tested these exact source blobs on hosted x86_64 Linux with stable rustc 1.99.1.
Four bounded controls passed in each feature profile, including a zero-byte
record-slot limit, callback reentrant `try_send`, a canceled overlapping
flush waiter and close with a held callback. The focused existing legacy
buffer-ownership suite passed 12 tests in default features; the existing
close-error suite passed six with all features; three legacy callback controls
also passed with all features. All-target, all-feature Clippy passed.
`cargo fmt --all -- --check` reported differences in both producer source
files, so the overall run failed. No full client suite was run.

A held error-callback control passed in both profiles on that run:
`close_timeout(150ms)` returned `Timeout` while the callback was held,
interceptor close did not overlap it, and one interceptor close occurred
after the callback was released. The injected pre-send error and mock broker
make this a controlled schedule, not a general shutdown proof.

The synthetic destructor unit control passed in both profiles. It held a
custom payload destructor and verified that the cutoff remained incomplete,
the finite permit remained unavailable and the owner lock remained usable;
the cutoff completed after the destructor was released. This injects a
test-owned payload after admission and is not a public payload history.

## Instrumented public cancellation controls

[Run 42687515915](https://github.com/mingley/partitionline/actions/runs/42687515915)
used qualification commit `85869a7408ee29ebf292066b74676bec2aa7552a`,
instrumented producer blob `9128b12cb3f0f2c8843254e38c9aefef61132f9e`,
the same `owned.rs` blob as above, test blob
`f2be9ab0e839a3a8c4060b4328745843b850d680` and mock blob
`4afd104e0b5e10227167d0d38328fe03dda07a0a`.
It adds a cfg-gated hold at an existing sequence-stamping yield and runs
against the in-repository mock.

Both profiles passed the released, aborted and timed-close controls with
mixed and all-send admissions. Each targeted batch contained 8,193 records.
Released controls observed 8,193 acknowledgement callbacks and no buffered
bytes. On abort or timed close, the mixed case observed one error receipt,
8,193 error callbacks and zero bytes; the all-send case observed 8,193 error
receipts, 8,193 error callbacks and zero bytes. Timed close returned
`Timeout`. The all-send fixture uses a long linger, flushes the warmup and
waits for each public queued metric; the mixed fixture uses zero linger.
Observation windows and injected scheduling limit the conclusion, and the
experiment closes admission after an orphan. A later send returning `Closed`
is not demonstrated sequence progress.

The [earlier instrumented run](https://github.com/mingley/partitionline/actions/runs/42685289855)
passed released and mixed abort/close controls in both profiles. Its all-send
cases failed before reaching the gate: a single poll did not mean admission
after the semaphore consumed its cooperative budget, and only 128 queued
records were observed. The later fixture explicitly waited for admission
rather than treating that precondition failure as a cancellation result.

## Admitted retry during close

A separate [public mock control](https://github.com/mingley/partitionline/actions/runs/42688304996)
used the same v3 producer source blobs and test commit
`432e964be205219db0d34ec2129dec30cde2b158`, with test blob
`472c4b4e67c4fa3e988563003b27a15b57aad798`. After a warmup, the mock
appended a target, delayed its response and dropped it. Close was polled
while that response was delayed. In both profiles the send and close
returned `Timeout`, no target retry was observed and the mock retained one
target value. The desired test failed. Source review identifies rejection of
already-admitted leases after the admission seal as a plausible cause, but
this run does not isolate the cause. An admission seal must be distinguished
from a fatal write fence, and admitted retries need a batch journal before a
repair can be qualified.

## Earlier failures and independent baseline

The [first candidate run](https://github.com/mingley/partitionline/actions/runs/42683870017)
compiled but all four bounded controls in both profiles failed at their
final close assertion with `Closed`. It also failed strict Clippy and
formatting. A subsequent
[run](https://github.com/mingley/partitionline/actions/runs/42685102989)
passed the four bounded controls in both profiles and targeted Clippy after
separating admission seal from worker shutdown, but formatting still failed.

Before the deferred-close change,
[run 42686602020](https://github.com/mingley/partitionline/actions/runs/42686602020)
failed the desired callback-ordering assertion in both profiles: an injected
pre-send failure held `on_error`, and timed close called interceptor
`close` while it was active. Close returned `Timeout`.

The new callback controls were also checked against the unchanged baseline.
Their [first run](https://github.com/mingley/partitionline/actions/runs/42682049454)
failed to compile because a condition-variable notification result was
unused; helper `expect` lints and formatting also failed. After correcting
the fixture, [run 42683258912](https://github.com/mingley/partitionline/actions/runs/42683258912)
passed three controls in both profiles and targeted Clippy, while the
fixture's formatting check still failed. Those baseline results do not
establish the ownership repair.

## Work still required

- Journal immutable whole-batch membership and a checked nonreused
  reservation before stamping or writing. Keep each retry as a sealed batch
  through retry routing, selection and individual timeouts.
- Fence each partition across old and new workers and distinguish an
  unwritten request from a partial or ambiguous write. Serialize identity
  publication with actual write offers and in-flight settlement.
- Implement and verify sequence rollover, stale rollback and topic
  incarnation. Equal numeric bases are not batch identities; the
  [seeded membership controls](../producer-retry-membership-20261109/README.md)
  demonstrate that existing helper behavior.
- Resolve overlapping/canceled flush error history, close/retry races,
  callback panic and blocking, runtime shutdown, transactional behavior and
  lifetime of completion resources. The slot registry and supervisor scans
  need bounded scheduling work and performance measurement.
- Run the complete client test suite, real-Kafka retry/fencing/transaction
  controls, and paired performance comparisons on exact qualified source.

No crate was published and no production source was changed on main by this
experiment.


## Reproduce the recorded controls

The source patch and fixtures are stored beside this note. The
[patch check](https://github.com/mingley/partitionline/actions/runs/42689316658)
ran `git apply --check`, applied it to the stated baseline and verified both
candidate source blobs. It also copied the fixtures, ran the test-only gate
injection and verified the instrumented source hash. It did not rerun Rust
tests. This is an experiment; apply it only in a separate worktree.

From a checkout containing this evidence directory, copy the directory into a
detached baseline worktree before applying the patch:

```sh
git worktree add --detach /tmp/partitionline-owner 731eb97a08445a18da0b043986ae192397ba176a
mkdir -p /tmp/partitionline-owner/docs/evidence/client
cp -R docs/evidence/client/producer-bounded-ownership-attempt-20261109 /tmp/partitionline-owner/docs/evidence/client/
cd /tmp/partitionline-owner
evidence=docs/evidence/client/producer-bounded-ownership-attempt-20261109
git apply --check "$evidence/producer-ownership.patch"
git apply "$evidence/producer-ownership.patch"
git hash-object src/producer.rs src/producer/owned.rs
```

The last command should print `f2a0384c0da06860f24d26c78745c9100757e513`
and `ea0ca2f02a786455b36b64e15a539e2d9ad947be`. Copy the three
ordinary fixtures and run the focused controls:

```sh
for name in producer_owner_bounded_controls producer_owner_reentrancy_controls producer_owner_close_callback; do
  cp "$evidence/fixtures/$name.rs.txt" "tests/$name.rs"
done
cargo test --locked --test producer_owner_bounded_controls -- --nocapture --test-threads=1
cargo test --locked --all-features --test producer_owner_bounded_controls -- --nocapture --test-threads=1
cargo test --locked --test buffer_ownership
cargo test --locked --all-features --test producer_close_errors
cargo test --locked --all-features --test producer_owner_reentrancy_controls -- --nocapture --test-threads=1
cargo test --locked --test producer_owner_close_callback -- --nocapture
cargo test --locked --all-features --test producer_owner_close_callback -- --nocapture
cargo test --locked --lib cutoff_waits_for_payload_destruction_outside_owner_lock -- --nocapture
cargo test --locked --all-features --lib cutoff_waits_for_payload_destruction_outside_owner_lock -- --nocapture
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

The recorded formatting check failed. Separately, the close/retry control
below failed in both profiles as described above:

```sh
cp "$evidence/fixtures/producer_owner_close_retry.rs.txt" tests/producer_owner_close_retry.rs
cargo test --locked --test producer_owner_close_retry -- --nocapture
cargo test --locked --all-features --test producer_owner_close_retry -- --nocapture
```

To reproduce the instrumented public cancellation controls, copy their
fixture and insert the cfg-gated sequence-stamp hold. This changes
`src/producer.rs` from the uninstrumented candidate and should produce the
recorded instrumented blob `9128b12cb3f0f2c8843254e38c9aefef61132f9e`:

```sh
cp "$evidence/fixtures/producer_ownership_regression_public.rs.txt" tests/producer_ownership_regression_public.rs
python3 "$evidence/inject-sequence-gate.py"
git hash-object src/producer.rs
for profile in default all; do
  flags=()
  if [ "$profile" = all ]; then flags+=(--all-features); fi
  for action in resume abort close; do
    for admission in mixed all; do
      env RUSTFLAGS='--cfg sequence_owner_witness --check-cfg=cfg(sequence_owner_witness)' \
        PL_OWNER_CASE="$action" PL_ADMISSION="$admission" \
        cargo test --locked "${flags[@]}" --test producer_ownership_regression_public \
        accepted_records_settle_after_worker_cancellation -- --exact --nocapture
    done
  done
done
```

These commands require Bash for arrays. The test gate holds a real cooperative
yield and validates the stamped prefix, but changes scheduling. The
all-send case uses a long linger and observes every admission. None of these
commands establishes a real-Kafka history or a performance comparison.
