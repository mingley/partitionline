# Classic group join with the original caller

This is an unrun diagnostic candidate pinned to client commit
`45cbff24de76490594970fc962cd04e7be9fafb1` and the Kafka 4.1.2 image
`apache/kafka:4.1.2@sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e`.
The optional library experiment boxes the private rejoin future; it is not an
adopted fix.

`classic-helper-stop.patch` changes only the test's `join_group` helper. When
classic `ConsumerGroup::join` returns successfully, the helper prints
`PL_CLASSIC_WITNESS_JOIN_SUCCEEDED` and terminates the test process. The original
`run()` caller, its later code and `live_compatibility_required` source remain
byte-for-byte unchanged. The process exit is unconditional on the first classic
join, so no later group, share, transaction, ReadCommitted, rollback, or history
scenario can run. The harness exits after the exact test command; the broker is
removed by the harness or its ownership-checked supervisor. Process exit skips
Rust destructors and the normal test teardown. This is a diagnostic boundary,
not a successful full libtest or compatibility result. The unchanged caller
still runs its admin, produce, and manual-consumer Fetch phases before reaching
classic join. That manual consumer uses the pinned default ReadUncommitted
isolation. This witness does not validate a particular negotiated Fetch version
or the interrupted v18 audit. The exit patch must never be applied to production
source.

The guard changes the helper's generated code and may change its future layout
or stack use. Each hosted job first compiles a non-polling ignored test against
the original helper, then against the guarded helper. In an explicitly sized
8 MiB diagnostic thread it constructs and drops the caller and join-helper
futures without polling them or connecting to a broker. It reports their sizes
and alignments. The live witness is skipped if either measurement differs.
Matching sizes and alignments do not prove identical internal field layout,
future construction, generated code, poll stack, timing, or original behavior.
Compiler and Cargo versions, source hashes, synthetic commits, patches, test
binary hashes after exit, logs, and process/container cleanup are retained.
The collector creates a new output directory exclusively; existing output is
rejected. It records `rustc +stable -Vv` and `cargo +stable -V` from the pinned
subject directory. Cargo and broker-harness commands use an owned process
session; the short Git, version and Docker inspection/removal probes use bounded
subprocess calls without session ownership. A probe timeout cannot establish
that every descendant stopped, and is recorded as unverified ownership.
Container cleanup checks the exact random name, label and ID; an unconfirmed
cleanup is inconclusive. Job loss can interrupt receipt or artifact retention.

The two cases are unmodified group code and the private `rejoin_once` box. They
run with the ordinary test stack setting on separate x86_64 hosted runners, not
as a performance comparison. The private box may allocate once per rejoin
attempt, retain heap memory across an await, add indirection, and still construct
stack temporaries before the allocation; optimization and costs are unmeasured.
It does not change the existing 20-second join or 150-second outer deadlines.

In earlier workflow 41674604055, a different early-return diagnostic reported
runtime stack overflow for normal and caller-box cases; stack-8-MiB and library-box
cases returned from classic join once each. The 2-MiB case overflowed during
rustls compilation, before its live test, while rustc inherited the old driver's
stack setting. That run did not establish whether the setting caused the failure.
Those separate single runs are not evidence that the boxed candidate fixes the
unchanged full compatibility test.

The workflow runs after a `main` push that changes its workflow file or this
capsule, or by manual dispatch. It compares the active workflow to this copy and
embeds hashes for all executable controls and patches; changing those inputs
requires a corresponding workflow update. Do not integrate or run it until its
stop boundary, controls, pins, and process ownership receive independent review.
After review, inspect its logs and retained artifacts for both cases. A live
witness result requires exit code zero and exactly one classic-join marker; it
must not be interpreted as a completed libtest. A missing marker, size mismatch,
compiler failure, timeout, allocation failure, resource failure, or incomplete
cleanup is inconclusive. Full compatibility, cancellation, allocation effects,
performance and production readiness require separate evidence.

Local checks are source-only. No Rust compile, live Kafka or benchmark ran under
the 1.5 GiB free-space floor. To rerun the static checks with a checkout containing
the pinned commit:

```sh
PL_STACK_SOURCE_CHECKOUT=/path/to/partitionline python3 -B -m unittest -v test_stack_runner.py
```
