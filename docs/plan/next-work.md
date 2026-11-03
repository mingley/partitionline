# Next work

Updated 3 November 2026. Use latest stable Rust and push reviewed changes
directly to `main`. The [task registry](tasks.json) remains unchanged:
249 done, 13 in progress and 84 pending.

## Qualification state

Production readiness and fastest-client/server claims remain unqualified.
Client 0.1.0 is already published. The next client version must be at least
0.2.0; broker and schema publication remain disabled.

| Scope | Recorded result |
| --- | --- |
| Client default library on `main`, `e7660f6e5` | 2,168 passed, 28 failed; later integration targets did not run |
| Client all-feature library on `main` at that commit | 2,170 passed, 26 failed; later integration targets did not run |
| Isolated client v10 library in each profile | 2,280 passed; selected producer tests still failed |
| Qualified broker v2 default all targets, `209d0b068` | 720 passed |
| Qualified broker v2 all-feature all targets at that commit | 868 passed |
| Broker v2 strict Clippy, docs, inventory and six fresh-process Fetch deadline checks | Passed |
| Client capability lanes at that commit | 93 passed in each profile; 450 Java reverse checks in each |
| Main broker CI, capability profiles, strict Clippy, audit, dependency policy, docs and package jobs | Passed at `e7660f6e5` |
| Main client and live Kafka compatibility CI | Failed at `e7660f6e5` |
| Reconnect candidate | 14 unit controls and six integration targets passed; latest broad Fetch 167 passed, two failed, one ignored |
| Integrated-source package navigation | 20 documentation tests passed; extracted crate checked 30 documents, 225 local links, 51 pinned repository targets |
| Package consumers and publication dry run at `fe65ac783` | Passed; actual publication remains gated |
| New dependency floors | 14 metadata graphs and 11 resolver controls passed; GitHub audit and policy checks pass |
| Fault/continuity campaigns | No passing 24-hour or seven-day run |

The [client matrix](../evidence/client/clean-default-validation-20261028/README.md),
[CI snapshot](../evidence/releases/main-ci-fe65-20261030/README.md),
[reconnect record](../evidence/client/reconnect-versions-20261030/README.md),
[package checks](../evidence/releases/package-navigation-20261030/README.md)
and [dependency record](../evidence/security/dependency-floors-20261030/README.md)
retain the exact scopes. Passing opt-in probes alone does not prove external
peer coverage. The broker inventory now passes all 55 controls; fuzz
supervision passes its 12 short-run controls. The qualified broker changes
are adopted byte-for-byte from v2; see the
[epoch/journal record](../evidence/broker/epoch-journal-20261030/README.md).
Earlier Fetch EOF, joined-shutdown QueueFull and Raft observer parse failures
remain recorded and unexplained beyond the passing specific controls.
No long campaign is credited.
Current-commit green CI and the complete release gates remain open.

## Correctness priorities

1. **Producer ownership and order.** Admission, worker retirement, task
   ownership, shutdown barriers and retry cancellation are under qualification.
   Controls cover held permits, cached handles, concurrent final-handle drops,
   cancellation before a task's first poll, callback cleanup and accepted retry
   handoffs. Thirty-three focused producer and decoder controls now pass,
   along with strict all-target/all-feature Clippy. The broader candidate
   default library run reports 2,176 passed and 20 failed. Seven producer
   integration targets report 118 passed, 19 failed and two ignored. The
   incremental reservation fix also prevents a stalled growth loop above
   8,192 elements. Keep those failed suites open.
   The broader issue-order repair is a separate prototype. A retained history
   shows an earlier unsent record being overtaken and receiving Kafka error 45.
   Sequence allocation saturates at `i32::MAX` instead of wrapping modulo
   2^31. Repair whole-batch issue ownership, unresolved-prefix replay, identity
   transitions, socket completion and cancellation together. See the
   [producer diagnostic](../evidence/client/producer-retry-diagnostic-20261028/README.md).

2. **Consumer delivery and isolation.** Topic recreation still fails one
   ownership control. Cancellation and mixed-error recovery must preserve
   buffered records, delivered offsets, fetch cursors and aborted-transaction
   state together. A ReadCommitted retry can expose an aborted record when
   its interval state is not rolled back. An automatic cybersecurity review
   rejected a proposed validation action for that repair; it was not rerun,
   and the repair remains open.

3. **Remaining reconnect work.** Replacement sockets now negotiate their own
   ranges and authenticate. Name-based offset retries re-encode for both range
   bounds, preserve fields or fail unsupported, and share one deadline. UUID
   requests refuse downgrade. The phase 10 Fetch run still failed recreation
   ownership and join/reset-leader overlap. Fifteen distinct historical failures
   include earlier timing and timeout cases; later focused passes do not close
   them. Large v10 encoding and duplicate response-classification decoding
   remain follow-ups.

   The current CI also exposes seven socket lifecycle failures. The pending
   repair retires idle sockets before attempting replacement and makes the
   completed-dial tests wait for real ApiVersions and authentication. A separate
   live compatibility test aborts with stack overflow. Classic JoinGroup can
   return a transient coordinator error after only one rediscovery retry;
   its bounded retry loop still needs controls.

4. **Bounded decoding.** Extend protection beyond the covered Fetch,
   ApiVersions, record and heartbeat paths. The grouped-assignment decoder and
   small Share batch-reservation fixes are being validated with producer builds.
   Per-message logical budgets do not bound all retained capacities or total
   process memory. Check valid large inputs, malformed error ordering and
   cancellation without raising existing budgets to hide regressions.
   Admin response projection, assignment aggregation, commit filtering and
   empty producer-request shells have prepared changes without native proof.
   Cancellation of accepted producer payloads across grouping and encoding
   awaits remains a separate ownership defect.

5. **Broker replication and transactions.** The operator has an RF1 data path
   and a separate metadata-only quorum. Replicated partition data and native
   Kafka KRaft are absent. Durable accepted leader epochs, journal permissions
   and five buffer-reservation fixes passed focused qualification and are on
   `main`. A committed-watermark truncation guard and authenticated network
   integration remain open. Transaction checkpoints do not implement
   participant/marker propagation; marker and transactional-offset histories
   retain finite limits. See the
   [replication](../evidence/broker/replication-limits-20261028/README.md)
   and [transaction](../evidence/broker/transaction-checkpoint-20261028/README.md)
   records.

   A safe committed-prefix guard needs a current, partition-bound authority
   and proof that local bytes match the committed leader lineage. The
   [reviewed design](../evidence/broker/committed-prefix-20261102/DESIGN.md)
   records those prerequisites; neither the authority nor the guard exists.

## Validation and performance

Run clean-commit client, broker and schema default/all-feature suites,
all-target strict Clippy, formatting, rustdoc, package consumers and independent
peer histories after fixes. Test-lint cleanup passes the remote client
all-target/all-feature gate at `fe65ac783`. Preserve all failed, skipped and ignored
cases. The [release guide](../RELEASE.md) requires exact-commit CI, package
checks and interruption-recovery rehearsal before publication.

Both capability profiles pass at `209d0b068`: 93 cases, no failed or ignored
required tests, 450 Java reverse checks over 225 Rust pairs, and 234 Streams
exports. The two SDK servers remain separate and unrun; Streams Java reverse
coverage is also unqualified. The actual compact reports and independent
review are in the
[capability record](../evidence/client/capability-native-20261030/README.md).
The earlier ignored-fixture failure remains in the
[fixture gate record](../evidence/client/capability-java-20261030/README.md).

The client v7 candidate ran 127 required library controls and 14 integration
controls within ten selected binaries in each feature configuration. Its
stack was 8 MiB except a separate 2 MiB mock group-lifecycle probe; live stack
safety remains open. Each library run had 2,253 passes and one failure from a
test setup panic. Each Mock Produce run had 31 passes and 15 failures; each
retained-backing run had seven passes, one timeout and one ignored test.
Strict Clippy found two test-only errors. Two of 22 expected baseline outcomes
did not reproduce. This isolated candidate has not been adopted on `main`.
See the [v7 results](../evidence/client/candidate-native-c025-20261030/README.md)
and the [earlier native rounds](../evidence/releases/candidate-native-20261030/README.md).
The staged [v10 candidate](../evidence/client/candidate-20261030/v10/README.md)
adds producer ownership changes, a deterministic retained-backing fixture
and a separate handshake diagnostic. Both library profiles and strict Clippy
passed. The original 450 ms Mock Produce assertion still fails, and the
retained-backing test still times out without the mock timer. See the
[v10 results](../evidence/client/candidate-native-b1-20261102/README.md).
The [handshake audit](../evidence/client/producer-handshake-v10-20261103/README.md)
shows two sequential 250 ms response delays before metadata completes, so the
old 450 ms end-to-end assertion conflicts with the candidate's required
replacement-socket negotiation. The trace also shows speculative leader
connections expiring before metadata returns. The assertion remains unchanged;
neither connection reuse nor a production speedup has been established.
The earlier
[v8 run](../evidence/client/candidate-native-350-20261102/README.md) passed
2,280 library tests in each profile and strict Clippy, but failed Mock Produce,
retained backing and one baseline expectation.
An [unchanged-main comparison](../evidence/client/candidate-20261030/main-comparison-b1/README.md)
is running those producer binaries with the same stack and thread settings;
it preserves the original failure conditions.

The staged [v12 client candidate](../evidence/client/candidate-20261030/v12/README.md)
adds producer deadline-scan reductions and timeout diagnostics to that work.
It retains the old test limits and requires 166 library and 17 integration
controls in addition to 23 baseline cases. Its source reviews and patch
reconstruction passed; compilation, runtime and performance results are
pending. Vec prefix shifts and unordered or sticky deadline scans remain.

Broker v2 observed seven baseline failures and passed twelve fixed controls,
three asynchronous filesystem controls and full default/all-feature targets,
strict Clippy/docs and all 55 inventory controls. Three fresh-process Fetch
checks passed in each profile. OOM counters were unavailable. Those exact
source bytes are now adopted; ordinary broker CI passed on `b1b048809`.
This local replica model is not a production replicated cluster.

The main client and live Kafka jobs failed on `e7660f6e5` in
[CI run 41576616173](https://github.com/mingley/partitionline/actions/runs/41576616173).
The [failure index](../evidence/releases/main-ci-e766-20261103/README.md) lists
the affected library tests, with the earlier b1 run retained separately. Counts
vary between runs with the same Rust source; that variation is not a measured fix.
The Release-plz PR workflow also failed because its semver checker discovered
archived evidence manifests as duplicate packages. An ignore rule now hides
those manifests from that traversal. The subsequent workflow identified
`0.2.0` as the next breaking-change version, then received GitHub HTTP 403
while creating its PR branch; no publication occurred.
The earlier release rehearsal self-test was hard-coded to `0.1.0`, and the
registry probe could treat an unavailable index as an absent version. V4 fixes
both in source and passes offline review for `0.1.0` and `0.2.0`; the hosted
offline rehearsal passed at `e7660f6e5`. The workflow's post-upload confirmation now requires a matching
API and index result, with offline controls for malformed responses. The local
owner-cut installability check still permits index-only confirmation; align it
with the dual-source rule before publishing. The
[release tooling record](../evidence/releases/release-readiness-20261102/README.md)
retains the failures, earlier candidate and v4 results. None is a publication.

The workspace has a four-CPU quota and limited disk. Serialize builds and
timing runs, retain source/executable hashes, and monitor a 1.5 GiB free-space
floor. The latest local broker baseline build hit that floor before any tests
ran; it is incomplete, not a test result. Native candidate qualification moves
to the existing GitHub runners without lowering the local guard. Static
source/evidence writes can leave the workspace below the compile floor; no
local native work may start there. Inactive Cargo cache files may be reclaimed
only after exact compressed
retention is verified. Do not remove immutable executable archives, failed
campaigns or their inputs. Candidate files under `/workspace/work` are local
scratch and are not promised as repository downloads.

The retained epoch-recovery timeout after 2,704 seconds remains unexplained.
Successful exact-seed replays and different-limit diagnostics do not replace
the failed campaign. No campaign is running.

Benchmark scope is this shared x86_64 host, without extra machines or an
external operator. A source-bound, one-partition closed-loop diagnostic is
prepared but unbuilt and unrun. Its supervisor has passing synthetic controls;
that is process-cleanup evidence, not a performance result. Keep any eventual
numbers limited to their settings, peers and architecture. The
[benchmark contract](../benchmark-contract.md) governs broader claims.
The producer's pending queue can rescan deadlines and shift a vector on each
small batch, creating quadratic work when a long queue drains one record at a
time. Replica epoch publication rewrites the journal on epoch changes. Measure
both under representative workloads after preserving ordering, cancellation,
crash safety and durability. Neither has a measured end-to-end speedup yet.
The [queue candidate](../evidence/perf/producer-pending-20261102/README.md)
skips certain deadline scans, but still needs native tests and benchmarking.
It does not remove vector prefix shifts.
The [broker epoch design](../evidence/perf/broker-epoch-20261102/DESIGN.md)
is unimplemented: a smaller durable sidecar changes migration and backup
semantics, and cannot preserve the current full-file mutation check without
additional validation.

The TLS graph still includes Ring C/assembly. The dependency update keeps that
provider. The passing audit applies to its checked lockfile and advisory
database revision; it does not establish an all-dependency Rust-only claim.
