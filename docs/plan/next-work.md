# Next work

Updated 30 October 2026.
Use the latest stable Rust and push reviewed changes directly to `main`.
The [task registry](tasks.json) remains the list of cards; no card is closed by
this handoff.

## Qualification state

- Client default all-target tests at `53b5af47f`: 3,354 passed, 76 failed,
  37 ignored; eight targets failed. The complete [logs and failure index](../evidence/client/clean-default-validation-20261028/README.md)
  are retained. The all-feature client matrix has not passed.
- Broker default all-target tests at `de38cd3df`: 687 passed. All features:
  830 passed, one OIDC test failed. The [broker receipt](../evidence/broker/clean-validation-20261028/README.md)
  explains its incomplete sequence, hash-only executables, and live probes
  that did not establish external coverage. Later [OIDC ordering checks](../evidence/broker/oidc-revocation-order-20261028/README.md)
  passed on an integrated dirty tree; rerun the full broker gates on a clean
  current commit.
- Production readiness and fastest-client/server claims are unqualified.
  No 24-hour fault campaign or seven-day continuity run has passed. The
  retained epoch-recovery timeout remains unexplained.
- Later [buffer reservation checks](../evidence/perf/buffer-reservations-20261030/README.md)
  restored the codec allocation gate and corrected 219 reservation calls.
  Default protocol tests reported 1,385 passed and two existing failures;
  scoped codec tests and strict Clippy passed. No throughput comparison ran.
  The [release guide](../RELEASE.md) now requires default and all-feature tests
  and interruption-recovery rehearsal before a new release.

## Correctness priorities

1. Fix producer ordering and ownership. A deterministic retry test showed a
   later record overtaking an earlier unsent record and receiving Kafka error
   45. Sequence allocation also saturates at `i32::MAX`; Kafka Java wraps to
   zero. A safe fix needs per-partition issue ownership across worker and
   leader changes, epoch-bound sequence reservations, cancellation-safe batch
   transfer, and bounded shutdown. See the [diagnostic and lane design](../evidence/client/producer-retry-diagnostic-20261028/README.md).
   A separate incremental-reservation loop can stop growing a cleared vector
   above 8,192 elements. Its completion controls and worker-retirement fixes
   are prepared locally but remain unqualified.
2. Fix consumer delivery and isolation across errors and cancellation. A
   ReadCommitted retry can expose an aborted record when aborted-interval
   state is not rolled back. Poll cancellation can also lose or reorder records
   removed from pending buffers before delivery. Validate offsets, buffered
   records, aborted state and delivery as one ownership transition.
   An automatic cybersecurity review rejected a proposed validation action for
   the ReadCommitted rollback repair. An already-running test had exposed the
   defect; the rejected validation was not rerun, and the repair remains open.
3. Complete reconnect and version negotiation. Replacement sockets must use
   their own negotiated ranges; name-based offset requests need safe
   re-encoding when a coordinator changes versions. Preserve UUID identity,
   deadlines, socket ownership and cancellation. A separate join/OffsetFetch
   overlap draft is unsafe until parked Fetch and idle-watcher ownership are
   handled. An earlier broad isolated Fetch replay had 141 passed, 27 failed and
   one ignored; its draft was not integrated.
4. Extend bounded decoding beyond the covered Fetch, ApiVersions, record and
   group-heartbeat paths. Per-message budgets do not establish a process-wide
   retained-memory bound. Check valid large messages, malformed inputs and
   cancellation without weakening wire compatibility.
5. Complete broker replication and transactions. The operator supports an
   RF1 data path and a separate metadata-only quorum, not replicated partition
   data or native Kafka KRaft. The local replica log still needs durable leader
   epoch fencing, a committed-watermark guard and network integration. The
   transaction coordinator checkpoint does not implement participant/marker
   propagation; marker and transactional-offset event histories still have
   finite retention limits. See the [replication](../evidence/broker/replication-limits-20261028/README.md)
   and [transaction](../evidence/broker/transaction-checkpoint-20261028/README.md)
   evidence.

## Validation and performance

Run clean-commit client and broker default/all-feature suites, all-target strict
Clippy, formatting, rustdoc, packaging and independent peer histories after
fixes. Preserve failures and ignored cases. The host has a four-CPU quota and
limited disk; avoid concurrent timing runs, monitor free space, and retain
source and binary hashes. Test outputs and candidate patches in `/workspace/work`
are local scratch, not part of the repository or guaranteed to persist.

The current authorized benchmark scope is this shared x86_64 host, without
additional machines. A source-bound, single-partition closed-loop diagnostic
was prepared locally but its Rust driver was not built and no campaign ran.
It cannot establish a global fastest claim. The [benchmark contract](../benchmark-contract.md)
still governs broader claims, including independent reproduction.

The default TLS dependency graph includes Ring's C/assembly; a strict
all-dependency Rust-only claim is not established. See the [provider review](../evidence/security/rustls-provider-review-20261028/README.md).
