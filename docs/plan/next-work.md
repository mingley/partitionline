# Next work

Updated 8 November 2026. Use latest stable Rust and push reviewed changes
directly to `main`. The [task registry](tasks.json) remains unchanged:
249 done, 13 in progress and 84 pending.

## Qualification state

Production readiness and fastest-client/server claims remain unqualified.
Client 0.1.0 is already published. The next client version must be at least
0.2.0; broker and schema publication remain disabled.

The latest completed main [CI snapshot](../evidence/releases/main-ci-3f45-20261106/README.md)
is `3f45fe6255e72abc6b0c4488bc13117779c50787`. The Linux client
library reported 2,180 passed/16 failed by default and 2,170/26 with all
features; Windows default reported 2,170/26 and macOS arm64 default 2,181/15.
The Kafka 4.1.2, 4.2.1 and 4.3.1 compatibility jobs overflowed after the
classic-join marker; 3.9.1 and 4.1.0 smoke jobs failed after EOS timeouts.
The broker passed 734 default and 882 all-feature tests, both capability
profiles passed their selected checks, and all 11 resource-soak driver
controls passed. The capability archives were not independently inspected.
These results do not cover later changes or close the release gates.

Earlier records:

| Scope | Recorded result |
| --- | --- |
| Client default library on `main`, `45cbff24d` | 2,177 passed, 19 failed; later integration targets did not run |
| Client all-feature library on `main` at that commit | 2,177 passed, 19 failed; later integration targets did not run. Failure identities differ from default. |
| Isolated client v10 library in each profile | 2,280 passed; selected producer tests still failed |
| Isolated client v14, 10 targets per profile | 2,431 passed, 17 failed and two ignored in each profile |
| Isolated client v16, 10 targets per profile | 2,432 passed, 16 failed and two ignored in each profile |
| Qualified broker v2 default all targets, `209d0b068` | 720 passed |
| Qualified broker v2 all-feature all targets at that commit | 868 passed |
| Broker v2 strict Clippy, docs, inventory and six fresh-process Fetch deadline checks | Passed |
| Client capability lanes at that commit | 93 passed in each profile; 450 Java reverse checks in each |
| Main broker CI at `45cbff24d` | 734 default and 882 all-feature tests, strict Clippy, docs, inventory and fuzz supervision passed |
| Main capability profiles at `45cbff24d` | Both passed 93 cases; SDK public-runtime qualification remains false |
| Main strict Clippy, audit, dependency policy, docs and package jobs | Passed at `45cbff24d` |
| Main client CI | Failed at `45cbff24d` |
| [Main live Kafka compatibility CI](../evidence/releases/main-ci-45cb-20261103/README.md) | Kafka 4.1.2, 4.2.1 and 4.3.1 runs at `45cbff24d` aborted with a stack overflow during classic group join |
| Windows default library at `45cbff24d` | 2,161 passed and 35 failed |
| macOS arm64 default library at `45cbff24d` | 2,179 passed and 17 failed |
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
The [earlier broker runs](../evidence/broker/main-ci-fdaf-20261103/README.md)
failed both three- and five-node Raft runtime tests with `ParseIntError`;
an observer test separately reported `NotLeader`. A test-only diagnostic
passed its three added controls in the default library run and exposed a
`NotLeader` response during a five-node leader-removal test. The underlying
cause and all-feature qualification remain open. A
[stale-leader control](../evidence/broker/raft-stale-selection-20261103/README.md)
tests one pre-append rejection path and passed natively; it does not justify
retrying every `NotLeader` response. Its bounded-read Clippy correction passed
the [hosted broker gate](../evidence/broker/main-ci-b1cf-20261103/REPORT.md)
at `b1cfbfd96`. The later [main broker run](../evidence/broker/main-ci-2645-20261103/REPORT.md)
also passed both profiles with the capacity fix.
No long campaign is credited.
The [45cb CI record](../evidence/releases/main-ci-45cb-20261103/README.md)
retains the exact failing identities and qualifications. It is a snapshot, not
evidence for later commits. Current-commit green CI and the complete release
gates remain open.

The [typed Raft attempt draft](../evidence/broker/raft-attempt-outcomes-20261103/README.md)
needs a current-source rebase, API compatibility decision, bounded test reads
and public Handle timeout and cancellation controls before adoption. Its review
also identified an existing runtime proposal-buffer capacity accounting gap.
An isolated [capacity fix candidate](../evidence/broker/candidate-20261030/runtime-capacity-v2-r2/README.md)
reproduced the old deadline outcome and passed its fixed tests on hosted x86.
The [hosted run](../evidence/broker/runtime-capacity-native-e2a0-20261103/README.md)
passed 729 default and 877 all-feature tests, along with formatting, strict
Clippy, documentation and the existing focused controls. Both doctest commands
found zero tests, and OOM counters were unavailable.
The qualified patch is applied. Inputs whose length is valid but whose capacity
exceeds the configured command payload envelope are now rejected before
admission. Caller-owned waiting futures remain outside the configured estimate.
The first harness failed inside a Git checkout; its corrected version passed.
The separate [controller queue capacity review](../evidence/broker/controller-queue-capacity-20261103/README.md)
found that its configured queue budget does not cover both retained vectors.
A [candidate and hosted controls](../evidence/broker/controller-queue-capacity-candidate-20261103/README.md)
bound queued input capacities but have not run natively. Natural pipelined TCP,
busy-actor cancellation, total process memory and performance remain open.
The [first hosted attempt](https://github.com/mingley/partitionline/actions/runs/41674601318)
failed before starting a job. Its job-level environment used a runner context,
which GitHub does not allow there. The workflow has been corrected; its retry
started but [failed during qualification](../evidence/broker/controller-queue-native-4f0c-20261103/REPORT.md).
Its retained artifact could not be accessed from this workspace, so the failure
cause and individual test outcomes remain unknown.
The source-reviewed [controller shutdown candidate](../evidence/broker/controller-shutdown-20261103/README.md)
now has a [hosted qualification capsule](../evidence/broker/candidate-20261030/controller-join-v2-r1/README.md).
Its [hosted job](../evidence/broker/controller-join-native-664b-20261103/README.md)
failed the broker inventory and inventory-negative-control gates. The pinned
runner's control flow indicates the earlier synthetic controls completed
without recorded gate errors, but their individual logs and the inventory
failure cause are unavailable. A local reconstruction reproduced a stale
controller-source hash. The [r2 candidate](../evidence/broker/candidate-20261030/controller-join-v2-r2/README.md)
updates that candidate-only registry pin; the [local validation](../evidence/broker/controller-join-inventory-r2-validation-20261103/README.md)
passed the inventory and all 55 unchanged Python controls. The [r2 hosted job](../evidence/broker/controller-join-native-r2-7e91-20261103/README.md)
reported a successful qualification; the individual command reports and
resource receipts were inaccessible, so gate details follow from the pinned
runner's control flow. The shutdown and registry patches are applied. The
one-off r2 lane is retired, and v17 now checks out the historical `7e91e2b`
source because its catalog pins the earlier registry. The [fresh main broker CI](../evidence/broker/main-ci-0ef1-20261103/README.md)
passed both profiles; real controller failure, WAL and transport coverage
remain open.

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
   A separate [AddPartitions review](../evidence/client/add-partitions-fatal-20261103/README.md)
   found that an earlier retriable partition error can hide a later fatal one.
   Its draft is uncompiled and unrun; missing response identities and
   cancellation before a fatal result is observed also remain open. A later
   [coverage candidate](../evidence/client/add-partitions-coverage-20261103/README.md)
   checks request identities and error precedence. Its [hosted focused run](../evidence/client/add-partitions-native-4f0c-20261103/README.md)
   reproduced both baseline witnesses and passed the candidate controls in
   both feature profiles, plus formatting and library Clippy. It is not
   applied; real Kafka, concurrent transaction ordering, request-side
   cancellation and performance remain open. The
   [first hosted attempt](https://github.com/mingley/partitionline/actions/runs/41674599283)
   failed before starting a job and used the same unsupported workflow context;
   the corrected retry produced the focused result above.
   Source review found that `flush` can miss delivery timeouts, lose a worker
   error when its caller is cancelled, or return before a retiring worker has
   handed retries off. Four focused controls [reproduced these failures](../evidence/client/producer-flush-native-86f2-20261103/README.md)
   in both feature profiles on the pinned base. The
   [source review](../evidence/client/producer-flush-controls-20261103/REPORT.md)
   describes the ownership transitions. Repair the barrier and error ownership
   together; the controls do not cover every lifecycle path. A
   [design and independent review](../evidence/client/producer-flush-design-20261103/README.md)
   record unresolved cleanup, cancellation, transaction and liveness choices;
   the sketch and small abstract model are not an implementation.
   Source review also found that repeated `begin_transaction` clears the open
   transaction's partition tracking. After an abortable failure, cancellation
   during identity recovery can clear the bump flag before a replacement
   identity is known, while another begin is permitted. Guard admission, end
   intent, cleanup and recovery as one lifecycle; a boolean CAS alone does not
   resolve these races. These findings have not been reproduced in a native or
   broker test.

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

   The latest CI also exposes socket and dial lifecycle failures. The pending
   repair retires idle sockets before attempting replacement and makes the
   completed-dial tests wait for real ApiVersions and authentication. A separate
   live compatibility test aborts with stack overflow. The [stack investigation](../evidence/client/live-compat-stack-2645-20261103/README.md)
   records the available phase evidence and unrun diagnostic experiments;
   the overflowing frame and a fix are not established. A separate
   [classic-join diagnostic](../evidence/client/live-stack-native-86f2-20261103/REPORT.md)
   ran once per arm on x86 with a shortened caller. Normal and caller-boxed
   tests overflowed; the 8 MiB and library-boxed cases returned. The 2 MiB
   setting affected rustc and failed during compilation, so it provides no
   runtime result. The trace overflowed after entering the classic connection
   path but before the next marker. None qualifies the unchanged compatibility
   test or proves the cause. The [revised diagnostic](../evidence/client/live-stack-native-4f0c-20261103/REPORT.md)
   confined stack overrides to the test process: the requested 2 MiB case
   overflowed at runtime and the 8 MiB shortened join returned. These remain
   single diagnostic observations, not a full compatibility test. An
   [original-caller witness](../evidence/releases/main-ci-19fef2c-20261103/README.md)
   reported stack overflow in both baseline and library-box arms. Its helper
   stops after classic join and cannot establish the unmodified test's full
   behavior; the logs report matching future sizes and alignments, which do
   not prove identical layout or stack use.
   Classic JoinGroup can
   return a transient coordinator error after only one rediscovery retry;
   its bounded retry loop still needs controls.

   A [metadata retirement candidate](../evidence/client/metadata-retirement-20261103/README.md)
   has source review and patch reconstruction but no native results. Its
   deferred task depends on runtime progress, and cancelling close before
   worker shutdown remains a separate lifecycle gap.

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

   The [Produce cancellation fix](../evidence/broker/produce-cancel-20261108/README.md)
   rolls back a canceled request's unsynced monolithic stages. Its x86
   qualification reproduced the prior failure and passed three controls in
   each profile, 737 default and 885 all-feature broker tests, strict Clippy
   and inventory checks. Rolling storage may retain a durable prefix;
   transport cancellation, truncation failure and power loss remain open.

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
passed in run `41574318102`. A repeat in run `41576616151` had 2,279 passes
and one failure in each library profile: the produce-topic encoding fairness
control. Both runs retain the same 15 Mock Produce failures and the
retained-backing timeout. The original 450 ms Mock Produce assertion still
fails, and the retained-backing test still times out without the mock timer. See
the [first v10 run](../evidence/client/candidate-native-b1-20261102/README.md)
and the [repeat](../evidence/client/candidate-native-e766-20261103/README.md).
The [handshake audit](../evidence/client/producer-handshake-v10-20261103/README.md)
shows two sequential 250 ms response delays before metadata completes, so the
old 450 ms end-to-end assertion conflicts with the candidate's required
replacement-socket negotiation. The trace also shows speculative leader
connections expiring before metadata returns. The assertion remains unchanged;
neither connection reuse nor a production speedup has been established.
A [group coordinator cache prototype](../evidence/client/group-coordinator-cache-20261103/README.md)
has four reviewed socket ownership and shutdown blockers and must not be
adopted until they are repaired and tested.
The earlier
[v8 run](../evidence/client/candidate-native-350-20261102/README.md) passed
2,280 library tests in each profile and strict Clippy, but failed Mock Produce,
retained backing and one baseline expectation.
The [unchanged-main comparison](../evidence/client/main-comparison-b1-20261103/README.md)
ran those producer binaries with the same stack and thread settings. Main
failed all 15 Mock Produce identities that fail in v10, plus three others,
and the retained-backing test timed out. Fixture differences mean the shared
identities do not by themselves prove the same cause.

The [v12 client attempt](../evidence/client/candidate-native-fdaf-20261103/README.md)
failed Clippy and the default test build because a test module did not import
`PendingWork`; no fixed runtime controls ran. Its successor v13 retains the
same client source with reviewed release-tooling inputs, so that compilation
error is corrected in the staged
[v16 candidate](../evidence/client/candidate-20261030/v16/README.md), which
retains v14's production Rust changes, 166 library and 17 integration controls,
23 baseline cases and the original timing limits. Its fixed-only deadline
fixture forces the unordered scan that its cancellation assertion exercises;
the unavailable v14 panic text leaves that earlier failure's cause unconfirmed.
The [v16 hosted run](../evidence/client/candidate-v16-job-20261103/REPORT.md)
passed format, strict Clippy, both builds and all 2,293 library tests in each
profile. Each profile still failed 15 Mock Produce tests and one retained-
backing test. The original 450 ms send-all test reported 756.0635 ms and
756.451082 ms; these mock-test timings are not a peer benchmark. The retained-
backing projection reported `Timeout` without identifying the phase.
The [repeat at `e2a04e229`](../evidence/client/candidate-v16-repeat-e2a0-20261103/README.md)
retained those integration failures and added one
all-feature library timing failure. Baseline case 23 was counted as an expected
failure even though it exited on broker error 14 before the intended
cancellation barrier. That result does not establish the barrier behavior.
The [phase-diagnostic follow-up](../evidence/client/retained-phase-followup-20261103/README.md)
is staged in the [v17 candidate](../evidence/client/candidate-20261030/v17/README.md)
with unchanged Rust tests and timing. Its [hosted run](../evidence/client/candidate-v17-native-2645-20261103/README.md)
failed the same 15 Mock Produce tests and retained-backing timeout in each
profile; both libraries passed. The console still did not identify the
timeout phase, and the raw compact artifact could not be inspected. V17 retains
the broad case 23 matcher, which needs a phase-specific witness and failure
provenance before the baseline result can qualify that control.
The [v17 helper guard](../evidence/client/v17-helper-pin-20261103/README.md)
checks helper hashes against its import manifest, but does not independently
pin that manifest: a paired helper and manifest edit passes the guard. The
triggering Git commit remains the provenance boundary. A successor must pin
the manifest and specification in the workflow and test paired changes.
A source-only successor draft adds those pins and a case 23 witness, but its
final independent integration audit was stopped by an automatic security
review that flagged the content as a possible cybersecurity risk. The draft
has not been applied or run.
The [v14 run](../evidence/client/candidate-v14-job-20261103/REPORT.md)
passed format, strict Clippy and both builds, but each profile failed the same
15 Mock Produce tests, one pending-deadline cancellation test and one
retained-backing test. The job log does not contain their panic diagnostics.
These profile counts overlap and must not be added as independent tests.
The [mock-produce source review](../evidence/client/v14-mock-investigation-20261103/REPORT.md)
maps the failing identities to their distinct assertions. Several expect
reuse of a live negotiated socket, while v14 opens a new socket and negotiates
it separately. V14's panic bodies were unavailable; v16 reported handshake
counts and latency assertions. Preserve socket-specific versions and
authentication when designing reuse. A reviewed [live-socket design](../evidence/client/live-socket-reuse-20261103/README.md)
sets out ownership and expiry controls; it is unimplemented and unmeasured.
Proposed [Worker write controls](../evidence/client/producer-worker-wire-controls-20261103/README.md)
would exercise partial and fully written frames on a gated TCP connection. They are
source-reviewed but uncompiled and unrun, and do not prove socket transfer or
retirement is safe.
The separate [main-source SASL expiry review](../evidence/client/sasl-expiry-boundary-20261103/README.md)
found that raw Produce writes bypass the connection expiry check. A fix must
preserve ownership and sequence state across earlier unanswered requests and
retries. No expired request was observed on a broker in that review.
The [stale-dial review](../evidence/client/producer-endpoint-stale-dial-20261103/README.md)
also identifies a v16 source path that can publish a dial after the node's
endpoint changes. Its proposed A→B and A→B→A controls are unrun; metadata
publication and already active workers need separate treatment.
The retained-backing fixture sends more than 4,096 single-record batches with
one request in flight. Repeated vector prefix drains shift the remaining
records; their contribution to the failure has not been measured. The reviewed
[deque design](../evidence/perf/producer-pending-deque-20261103/README.md)
explores avoiding those shifts while preserving order, cancellation ownership
and deadline accounting. Its isolated prototype is uncompiled and unmeasured;
the review found unresolved allocation-unwind and resource-bound issues.
The [v13 job log](../evidence/client/candidate-job-337-20261103/REPORT.md)
also reports failed Clippy and default builds; its compiler diagnostics and
report archive could not be retrieved through the available transfer path.
The [qualification runner](../evidence/client/candidate-diagnostics-20261103/README.md)
prints bounded failure context in the job log while retaining full reports. It
ran on v16; its console projection is not an independent proof and is not
secret-redacted.
The new baseline group control failed before demonstrating its intended
third-request barrier. Vec prefix shifts and unordered or sticky deadline
scans remain, and no performance change is measured.

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
offline rehearsal passed at `e7660f6e5`. The workflow and local owner paths now
require matching API and index identity before reporting confirmation. Consumer
checks bind the selected version and source to the target package, and release
paths fail closed on unconfirmed exact-SHA CI. The token check reports presence
without claiming authorization. One hundred offline tests passed on the final
patch, and the hosted offline rehearsal passed at `33744862c`. The installed
GitHub CLI 2.46.0 rejects the earlier checker's requested `attempt` JSON field.
Its [replacement](../evidence/releases/ci-attempt-binding-20261103/README.md)
binds jobs and artifacts to the run attempt. A [temporary-path follow-up](../evidence/releases/ci-colon-tmpdir-20261103/README.md)
passed the expanded 122-test offline suite. Live pagination and end-to-end
current-attempt verification remain open.
The [local release record](../evidence/releases/local-release-gates-20261103/README.md)
retains the new controls and original failures. The
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

At `44c4a36cb`, the [resource-soak CI job](https://github.com/mingley/partitionline/actions/runs/42131240635/job/127608095571)
failed its interrupted-run test because the validator found fewer than two host
samples. The cause is unconfirmed. The test now waits for complete application
and host samples before signaling and verifies that the attempt actually
interrupted; it does not change the driver's evidence requirements. The
[hosted control](https://github.com/mingley/partitionline/actions/runs/42132168250)
built the driver and passed all 11 resource tests. Its temporary sample output
was not retained; this is a focused test result, not a sustained campaign.

Benchmark scope is this shared x86_64 host, without extra machines or an
external operator. A source-bound, one-partition closed-loop diagnostic is
prepared but unbuilt and unrun. Its supervisor has passing synthetic controls;
that is process-cleanup evidence, not a performance result. Keep any eventual
numbers limited to their settings, peers and architecture. The
[benchmark contract](../benchmark-contract.md) governs broader claims.
The producer example screens phase-end completion counters before stopping each
timer; it is not a phase-isolation barrier. Individual outcomes and independent
verification are still required. Its matched-bulk warmup and ID handling need a
peer-matched verification method. The record-history converter now keeps
accepted status and uses a strict nontransactional receipt policy rather than
inferring individual acknowledgements from aggregate counters. The [hosted
controls](https://github.com/mingley/partitionline/actions/runs/42130958330)
passed 45 generic and 27 benchmark/shell tests. Full benchmark replay still
requires the original journals and offset fence, and eight-million-record
memory and runtime remain unmeasured.
The producer's pending queue can rescan deadlines and shift a vector on each
small batch, creating quadratic work when a long queue drains one record at a
time. Replica epoch publication rewrites the journal on epoch changes. Measure
both under representative workloads after preserving ordering, cancellation,
crash safety and durability. Neither has a measured end-to-end speedup yet.
The [queue candidate](../evidence/perf/producer-pending-20261102/README.md)
skips certain deadline scans, but still needs native tests and benchmarking.
It does not remove vector prefix shifts.
An isolated [full-drain candidate](../evidence/perf/producer-full-drain-20261103/hosted/README.md)
transfers matching-capacity vectors instead of copying a complete batch. Its
correctness-gated x86 null-broker comparison did not reach its correctness
gates or timing: [run 41676355649](https://github.com/mingley/partitionline/actions/runs/41676355649)
failed while preparing the pinned source archive. The log does not distinguish
archive failure from a timeout or free-space floor. The candidate does not
address partial drains or establish Kafka performance.
A [paired queue microbenchmark prototype](../evidence/perf/pending-queue-v16-20261103/README.md)
has source review. Its [v17 rebind](../evidence/perf/pending-queue-v17-20261103/README.md)
remains uncompiled and inactive until the case 23 baseline witness is repaired
and complete correctness passes in the same job. Synthetic completions do
not prove Kafka delivery.
The [supervised group coordinator draft](../evidence/client/group-coordinator-cache-20261103/README.md)
needs its eviction, close cancellation, retry ordering and admission limits
resolved before native qualification.
The [broker epoch design](../evidence/perf/broker-epoch-20261102/DESIGN.md)
is unimplemented: a smaller durable sidecar changes migration and backup
semantics, and cannot preserve the current full-file mutation check without
additional validation.

The TLS graph still includes Ring C/assembly. The dependency update keeps that
provider. The passing audit applies to its checked lockfile and advisory
database revision; it does not establish an all-dependency Rust-only claim.
