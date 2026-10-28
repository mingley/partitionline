# Producer retry and sequence diagnostics

The clean client matrix on commit `53b5af47f` found producer retry failures. A
separate sparse, dirty candidate based on `63aa045be` was built to investigate
those failures; it is not an integrated fix. On Linux x86_64 with stable Rust,
12 of 13 individually selected tests passed. They included two retry-actor
cancellation/accounting tests, worker-retirement tests, the previously failing
retry-exhaustion case, and six unchanged lost-ack cases. A new test failed:
a later record overtook an unsent retry and the mock returned Kafka error 45
(`OUT_OF_ORDER_SEQUENCE_NUMBER`). The mock enforces per-partition producer
sequences. This result leaves producer ordering unqualified.

The candidate also retries remote socket `Closed` at the Produce transport
boundary while retaining the public error classification for local shutdown.
Its in-hand retry batch guard passed the two selected cancellation cases, but
it invokes user interceptors synchronously on drop, the remaining channel drain
is not bounded by that guard, and no general cancellation-latency guarantee
follows. The candidate lacks an issue lane that controls initial sends and
retries across worker and leader changes. Passing the other selected tests does
not establish those guarantees. The full client suite was not rerun on this
candidate.

`build-01` failed on nine unused-result errors in new test fixtures;
`build-02` fixed those fixture errors and built successfully. The build and
execution receipts record a dirty sparse worktree, matching before/after input
hashes, exact commands and the source patch. The 13 tests used saved copies of
three freshly built executables; their hashes and local paths are in
`results.json`, but the executables are not included here. The runner checks
that each command selected exactly one test. A later v3 patch adds leader-change
and sequence-boundary tests but has not been built or run. Source identity does
not exhaustively cover external dependencies, the OS or the whole main tree.

Pinned Kafka Java 4.3.1 independently confirms producer sequence numbers wrap
modulo 2^31: `MAX + 1 = 0`, `(MAX - 1) + 2 = 0`, and `0 - 1 = MAX`. The
repository's record codec has wrapping arithmetic, while producer allocation,
stamping and rollback currently saturate. The oracle sources, outputs,
launcher and jar hashes, successful exit codes and before/after checks are in
`java/`. The first attempted source imported the old class path and failed to
compile; its output is retained and its process exit was not separately
recorded. The JDK modules hash was recorded after the runs only. These checks
cover the listed Java helper cases, not producer wire interoperability or
concurrent reservation safety.

The [lane design](lane-design.md) describes the required issue ownership,
sequence and epoch handling, worker admission gate, bounded cleanup, and peer
histories. A modular rewind alone cannot safely handle concurrent reservations
or cancellation. This archive does not qualify production readiness or a
performance claim.

Verify stored and reconstructed bytes from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/client/producer-retry-diagnostic-20261028
```
