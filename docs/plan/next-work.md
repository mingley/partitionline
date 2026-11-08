# Next work

Updated 8 November 2026. Use latest stable Rust and push reviewed changes
directly to `main`. The [task registry](tasks.json) remains unchanged:
249 done, 13 in progress and 84 pending.

## Qualification state

Production readiness and fastest-client/server claims remain unqualified.
Client 0.1.0 is already published. The next client version must be at least
0.2.0; broker and schema publication remain disabled.

The latest complete recorded main [CI snapshot](../evidence/releases/main-ci-47d-20261108/README.md)
is `47df27f9f3e04fc524849f3a9753c0b0469f8947`. Client libraries reported
2,229 passed and 15 failed on Linux default, 2,222 passed and 22 failed with
all features, 2,226 passed and 18 failed on Windows, and 2,229 passed and
15 failed on macOS arm64. Three Kafka compatibility jobs and both smoke jobs
failed; resource-soak passed. Broker CI passed 779 default and 927 all-feature
tests, with four ignored per profile, plus strict checks, inventory and fuzz
supervision. Both selected capability profiles passed; their archives were not
independently inspected.

The [preceding snapshot](../evidence/releases/main-ci-118-20261108/README.md)
also failed client checks and passed broker checks; it preceded the frame-read
change.

An [earlier snapshot](../evidence/releases/main-ci-c7-20261108/README.md)
also failed client checks and passed broker checks. These source commits differ
only in documentation.

An [earlier snapshot](../evidence/releases/main-ci-60c-20261108/README.md)
also failed client checks and passed broker checks. These source commits differ
only in documentation.

An [earlier snapshot](../evidence/releases/main-ci-262-20261108/README.md)
recorded a `/proc` file-descriptor permission error in resource-soak, leaving
that workload incomplete. Broker CI stopped in the default profile at 696
passed, one failed and four ignored after a three-node Raft `NotLeader`
response. Its all-feature suite, Clippy and Rustdoc did not run; inventory and
fuzz supervision passed separately. Post-failure node samples do not establish
the cause or whether the mailbox change contributed. The later passes do not
explain either failure. An [earlier snapshot](../evidence/releases/main-ci-02c-20261108/README.md)
retains two additional Raft failures.

An earlier [main CI snapshot](../evidence/releases/main-ci-440-20261108/README.md)
at `4409633` recorded 17 successful client jobs, nine failures, one
cancellation and one skip. The failed jobs include the Linux, Windows and
macOS client libraries, three current Kafka compatibility jobs, the Kafka
3.9.1 smoke and resource-soak. The soak log reports an OS permission error
for a process's `/proc` directory; its workload result is incomplete. The
Kafka 4.1.0 smoke was canceled when main advanced. Broker CI passed 764 default
and 912 all-feature tests, with three ignored in each profile. Strict checks,
inventory and fuzz supervision passed. Both selected capability profiles
passed; their archives were not independently inspected.

An [intervening snapshot](../evidence/releases/main-ci-f092-20261108/README.md)
at `f092d69` recorded 18 successful client jobs, five failures, four
cancellations and one skip. Its resource-soak job passed; that does not
explain the earlier `/proc` permission failure. Broker CI passed 766 default
and 914 all-feature tests, with three ignored per profile.

A [later partial snapshot](../evidence/releases/main-ci-364-20261108/README.md)
at `36424a5` recorded three failed current Kafka compatibility jobs and eight
canceled client jobs when main advanced. Its broker run passed 766 default and
914 all-feature tests, with three ignored per profile; those passes do not
explain the earlier `02c443c` Raft failures.

A [partial earlier client run](../evidence/releases/main-ci-e02-partial-20261108/README.md)
at `e02c1b8` recorded six failed jobs and four canceled when main advanced.
Linux default and Windows library tests failed, as did three Kafka
compatibility jobs and the Kafka 3.9.1 smoke job. The canceled jobs do not
have a qualification result.

Earlier [broker CI](../evidence/broker/main-ci-3fc-20261108/README.md)
at `3fc8059` stopped at two all-feature Raft runtime failures. A
[subsequent run](../evidence/broker/main-ci-c471-20261108/README.md) stopped
at a quorum integration test after a delete returned error 56. The retained
log does not show whether the delete committed or identify the cause. The later passes at `eaa8ac6` and `de152e7` do not explain these failures.

A [separate quorum diagnostic](../evidence/broker/quorum-delete-diagnostic-20261108/README.md)
captured one new Delete that completed and an earlier, distinct timed-out
Create whose token appeared in committed journal entries in three
post-shutdown copies. It did not reproduce or explain the original error 56.
A [leader-removal control](../evidence/broker/quorum-proposal-boundary-20261108/README.md)
shows one pre-append refusal; it does not establish that every NotLeader is
safe to retry.

A [later broker run](../evidence/broker/main-ci-e02-20261108/README.md) at
`e02c1b8` stopped on a distinct before-failover CreateTopics error 56.
The test did not retain the error message, topic ID, internal token, target
node or operation outcome. This failure is unresolved. A [test diagnostic](../evidence/broker/create-response-diagnostic-20261108/README.md)
now records the message, contacted node and previous 41 responses on future
unexpected Create failures; it does not determine commit outcome.

The [Raft liveness investigation](../evidence/broker/raft-liveness-20261108/README.md)
records a five-node short-election fixture that reached term 163 without a
stable leader, a separate observer timeout and later controls. The failure
did not recur in 27 fresh controls. Instrumented successful histories recorded
87 full vote queues, but the capture changed timing and did not establish the
failure's cause. The test candidate and instrumentation remain unapplied. A [vote-route control](../evidence/broker/raft-vote-route-20261108/README.md)
later forced one newer campaign to be discarded by a full one-slot route. A
later campaign recovered in the control; this does not explain the earlier
stall. The [mailbox repair](../evidence/broker/raft-vote-mailbox-20261108/README.md)
now keeps the newest pending vote and cancels the displaced correlation.
Its targeted and full broker checks passed, but it cannot interrupt an older
in-flight RPC or establish the cause of those earlier failures. A [stale-reply control](../evidence/broker/raft-stale-vote-term-20261108/README.md)
records different handling for expired and current correlations. Whether an
expired response should advance authority remains open; the private transport
does not cryptographically authenticate peers. An unmerged
[vote-dispatch candidate](../evidence/broker/raft-vote-dispatch-20261108/README.md)
offers all current votes before waiting on cancellation. A forced loopback
schedule reached two real peers while the source owner was blocked. Its final
all-feature broker suite failed a distinct five-node `NotLeader` test; the
cause and any relationship to the candidate remain unknown. Dispatch
cancellation can still leave an unavailable route's correlation registered.

The [coordinator repair](../evidence/client/group-coordinator-retirement-20261108/README.md)
retires a known coordinator socket before replacement. Focused local controls
and all 67 candidate network tests passed, but its baseline comparison job
failed. Live-broker failover and other authentication paths remain open.

A [failure-cleanup candidate](../evidence/client/producer-failure-cleanup-20261108/DEFERRED.md)
releases a canceled batch's resources, but can block a current-thread runtime
past a timer deadline while running a user callback. It remains unapplied; the
record describes bounded deferred cleanup requirements and a small local
performance diagnostic.

[Journal ownership controls](../qualification/journal-ownership-2026-11-08.md)
reproduced false inode-identity collisions for files, cursors and directories
on one x86 Linux host. The [repair](../evidence/broker/journal-ownership-repair-20261108/README.md)
pins inode identities through owner and staged-sync lifetimes. It passed both
full broker test profiles, with the isolated descriptor diagnostics run
separately. It adds descriptors and reference-count operations; process-wide
descriptor limits and broader fault qualification remain open. The controls did
not establish the cause of the earlier quorum failure.

The [grouped journal reservation repair](../evidence/broker/grouped-journal-reservations-20261108/README.md)
reserves the committed-index and staging-queue capacity before writing each
grouped record. Combined with the record parser, it passed 761 default and 909
all-feature tests, plus six separately run descriptor diagnostics. Later
descriptor cloning can still fail after a write and requires reopen. The
scoped benchmark was too noisy to establish a speedup.

An earlier [frame-read deadline test](../evidence/broker/frame-read-deadline-test-20261108/README.md)
returned its deadline error after 82.681715 ms against an 80 ms wall-time
assertion. The log does not distinguish scheduling delay from time inside a
read poll. The revised test keeps the deadline check and independently tests
yielding without a wall-time threshold; that test revision did not change
production transport. A later [framing change](../evidence/broker/frame-read-fairness-20261108/README.md)
bounds consecutive ready frame reads by bytes and calls. It passed both broker
profiles and focused controls, including a preloaded TLS stream. On one shared
x86 host, 256 KiB loopback throughput fell about 2% over TCP and 1.6% over
TLS/TCP. A one-byte read adapter had larger TLS cost. A
[follow-up](../evidence/broker/frame-read-fairness-20261108/network-rtt-followup.md)
retained individual RTTs and saw the TCP 256 KiB result change direction on a
different runner; the cause is unknown. These are short framing measurements,
not Kafka throughput or a global ranking. The synchronous complete-frame copy
and work inside one read poll remain outside this bound.

An [observer diagnostic](../evidence/broker/observer-image-diagnostic-20261108/README.md)
records image size and a bounded, non-atomic prefix sample when either
checkpoint assertion fails. Focused controls and combined broker suites
passed, but the earlier unexpectedly small image remains unexplained.

The [admission repair](../evidence/broker/fetch-owned-admission-20261108/README.md)
retains data permits through actor-owned requests, waiting Fetches and
unconsumed replies. The combined source passed both full broker test profiles
and separate descriptor controls. Its earlier default all-target transport
failure remains unexplained; a stronger fixture uses a longer finite deadline
and checks real admission before cancellation.

A [segmented-log crash control](../evidence/broker/segment-initial-crash-20261108/README.md)
found that a fresh log can fail to reopen after exit before initial manifest
publication. No append was attempted in that witness. An acknowledged-data
control with the manifest removed failed closed. Recovery and marker
provenance remain open.

A [data-buffer capacity candidate](../evidence/broker/data-buffer-capacity-20261108/README.md)
checks request and scratch capacity before enqueue and avoids recycling
oversized buffers. It is unapplied: the combined default and all-feature
broker runs failed Raft tests. In a scoped public-path measurement, freeing a
faulted-in 256 MiB caller spare took a 16.134 ms median in the caller's first
poll. Its logical admission budget is not a process RSS bound. An [off-runtime disposal design](../evidence/broker/data-buffer-capacity-20261108/off-runtime-design.md)
is unimplemented and requires explicit overload and shutdown semantics.

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
   both feature profiles, plus formatting and library Clippy. One
   [single-node Kafka 3.9.1 transaction](https://github.com/mingley/partitionline/actions/runs/42438994537)
   also passed with the candidate. It is not applied; real failure recovery,
   concurrent transaction ordering, error-state races, request-side
   cancellation and performance remain open. The
   [first hosted attempt](https://github.com/mingley/partitionline/actions/runs/41674599283)
   failed before starting a job and used the same unsupported workflow context;
   the corrected retry produced the focused result above.
   Source review found that `flush` can lose a recorded worker error when
   its caller is cancelled or return before a retiring worker hands retries
   off. [Focused controls](../evidence/client/producer-flush-native-86f2-20261103/README.md)
   exercise those cases in both feature profiles. A [public mock history](../evidence/client/producer-flush-public-20261108/README.md)
   paused a real worker at retry handoff and saw flush return while the record
   receipt remained pending; the gate changes scheduling and the mock is not
   Kafka. Individual delivery timeouts are not necessarily flush errors under
   the documented contract. An isolated [completion model](../evidence/client/producer-completion-horizon-20261108/README.md)
   is not connected to the producer. A [gated public draft comparison](../evidence/client/producer-flush-barrier-public-20261108/README.md)
   waited for the retired worker's record but still lost a worker error after
   a flush waiter was canceled. A separate [public flush history](../evidence/client/producer-flush-sent-20261108/README.md)
   shows an injected error can be sent to a live waiter, then discarded when
   that unpolled waiter is dropped while overlapping and later flushes return
   Ok. This uses a mock and does not settle every flush-error contract. An
   isolated [bounded error-state model](../evidence/client/producer-flush-error-model-20261108/README.md)
   still masks a worker timeout with a generic protocol summary and is not a
   repair. A proposed [ownership design](../evidence/client/producer-ownership-design-20261108/README.md)
   records the unresolved lifecycle, transaction and sequence requirements;
   the draft remains unapplied. The [retained-admission test](../evidence/client/retained-cancellation-test-20261108/README.md)
   now isolates one partial `send_all` cancellation and detects missing or
   duplicate release in a gated mock; producer code is unchanged. The barrier
   draft's short mock benchmark recorded higher median admission and flush
   times; it did not compare Kafka clients. A
   [private waker control](../evidence/client/producer-waker-handoff-20261108/README.md)
   found a double release under an injected panic; public reachability is not
   established. An isolated [sequence cancellation control](../evidence/client/producer-sequence-cancel-20261108/README.md)
   dropped an artificial batch owner after the actual helper reserved sequence
   numbers: receipt channels closed while the counter stayed advanced. This
   does not execute the public producer or Kafka I/O; safe batch ownership and
   recovery across partial writes remain open. A gated
   [public producer history](../evidence/client/producer-sequence-public-20261108/README.md)
   paused an idempotent batch before its last stamp. Public `close_timeout`
   returned `Timeout`, the retained receipt returned `Closed`, no batch
   callbacks were observed and 65,544 bytes stayed counted. In a separate
   forced worker abort, a replacement task sent the next batch with a sequence
   gap and the repository mock returned error 45. The gate changes scheduling;
   neither observation is a real-Kafka history. An
   [in-flight close control](../evidence/client/producer-inflight-close-callback-20261108/README.md)
   showed a blocking error interceptor delaying a 150 ms close to 751 ms on a
   current-thread runtime while a registered timer was also delayed. The
   record had been appended by the mock but its response was withheld. This
   is a controlled mock history, not a general latency result. Seeded private
   [rollover controls](../evidence/client/producer-sequence-rollover-20261108/README.md)
   confirm saturation where Kafka sequence arithmetic wraps. In a separate
   seeded public mock history, two different sends returned success at the
   same offset while the mock stored only the first value. A three-line
   arithmetic control stored both but is not an ownership or concurrency fix.
   Neither history is a real-Kafka result. A
   [cleanup-service review](../evidence/client/producer-cleanup-service-20261108/README.md)
   describes ownership, admission, write, callback and shutdown requirements;
   it is not an implementation. Separate
   [cancellation controls](../evidence/client/addpart-retryfix-264-20261108/REPORT.md)
   reproduced lost completion results and retained buffer bytes during a
   coordinator retry and during single-record failure cleanup. Those controls
   are synthetic and do not establish public shutdown behavior. The
   [source review](../evidence/client/producer-flush-controls-20261103/REPORT.md)
   describes the ownership transitions. Repair the barrier and error ownership
   together; the controls do not cover every lifecycle path. A
   [design and independent review](../evidence/client/producer-flush-design-20261103/README.md)
   record unresolved cleanup, cancellation, transaction and liveness choices;
   the sketch and small abstract model are not an implementation. A later
   [independent review](../evidence/client/producer-flush-review-20261108/README.md)
   adds sticky-cohort, starvation, overlapping-waiter and seal-incarnation
   requirements; these are source observations, not a tested repair.
   The [transaction lock fix](../evidence/client/txn-lock-deadline-20261108/README.md)
   includes the initial and rediscovery-snapshot waits in the request budget.
   The later [replacement fix](../evidence/client/txn-replacement-20261108/README.md)
   keeps the old coordinator owned during cancellation and bounds the final
   lock and retirement waits. It passed focused mock controls and a Kafka
   3.9.1 happy-path comparison. Response decoding, parts of discovery and
   epoch-bump waiting still exceed a complete deadline; concurrent
   discoveries can install a stale result. The [transaction error-state
   repair](../evidence/client/txn-error-state-374-20261108/REPORT.md) makes
   fatal and abortable claims visible before their cause lock and prevents
   cleared claims from overwriting newer ones. Transaction generations and
   already-admitted operations remain unguarded; three handshake-count tests
   still fail on both the baseline and candidate.
   Source review also found that repeated `begin_transaction` clears the open
   transaction's partition tracking. After an abortable failure, cancellation
   during identity recovery can clear the bump flag before a replacement
   identity is known, while another begin is permitted. Guard admission, end
   intent, cleanup and recovery as one lifecycle; a boolean CAS alone does not
   resolve these races. These findings have not been reproduced in a native or
   broker test.

2. **Consumer delivery and isolation.** A [nontransactional cancellation
   control](https://github.com/mingley/partitionline/actions/runs/42443409232)
   fetched a record while an interval commit remained pending. Canceling the
   poll advanced the position, and the retry returned no record at that offset.
   Fix and qualify ownership across the commit wait. Topic recreation still fails one
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

   The recorded CI also exposed socket and dial lifecycle failures. The applied
   known-coordinator repair retires the old socket before replacement; other
   dial paths remain open. A separate live compatibility test aborts with stack
   overflow. The [stack investigation](../evidence/client/live-compat-stack-2645-20261103/README.md)
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

4. **Bounded decoding.** The [Admin pagination repair](../evidence/client/admin-pagination-20261108/README.md)
   uses one checked deadline for DescribeTopicPartitions pages and rejects an
   immediately repeated cursor instead of returning a partial success. A
   four-partition Apache Kafka 4.1.2 smoke and focused controls passed in both
   profiles. The default broad Admin suite passed 604 tests; the all-feature
   suite had 602 passes and two 80 ms timing failures. Those failures remain
   unexplained, and an unchanged baseline also failed the same controls on a
   shared host. Longer cursor cycles can continue until timeout, and accumulated
   descriptions are not bounded by this change. A short three-way loopback
   comparison measured candidate/baseline median elapsed ratios of 1.01145 for
   single-page calls and 1.00551 for sixteen-page calls.
   The [Admin response reservation change](../evidence/client/admin-response-reservation-20261108/README.md)
   limits the initial capacity requested from `Vec` in seven response
   families. Valid results can grow beyond that request; other decoders and
   aggregate memory remain outside its scope. Two short diagnostics measured
   roughly 6% higher median decode time for 20,000 valid CreateTopics results,
   and some valid responses used more peak requested capacity. A separate
   [DescribeTopicPartitions reservation candidate](../evidence/client/admin-dtp-reservation-20261108/README.md)
   reduced initial requests in malformed inputs but remains unapplied: valid
   large-topic and large-partition decodes had higher median times in a short
   shared-host comparison. Results can grow beyond the initial request.
   Extend protection beyond the covered Fetch, ApiVersions, record and
   heartbeat paths. The grouped-assignment decoder and
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

   The [Fetch admission fixture](../evidence/broker/fetch-admission-20261108/README.md)
   replaces a scheduling-dependent first-poll assertion with a gated actor and
   checks both consumption and cancellation. A separate [queue witness](../evidence/broker/fetch-canceled-queue-20261108/README.md)
   demonstrates one possible cause of a later transport-shutdown `QueueFull`
   failure; that uninstrumented failure's cause remains unproven. The queue
   fixture passed its focused controls and 887 all-feature tests, while its
   full default run stopped at a quorum `Journal(AlreadyOpen)` failure.
   A [modeled cancellation overlap](../evidence/broker/fetch-actor-overlap-20261108/README.md)
   retained 8,192 bytes of input under a 4,160-byte configured ceiling.
   It models a possible actor schedule, not process RSS. The applied
   [admission repair](../evidence/broker/fetch-owned-admission-20261108/README.md)
   keeps the data permit with actor-owned work. Vector capacity, caller-supplied
   spares, actor caches, transport ownership and process RSS are not bounded
   by that logical payload budget.

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

A [closed-loop acknowledgment diagnostic](../evidence/client/ack-closed-loop-20261108/README.md)
completed individual deliveries and independent readback for two client
drivers. Its short runs have mismatched queue settings, timeout settings and
driver costs, so they are not a qualified client comparison.

Two newer [Kafka producer comparison attempts](../evidence/client/ack-matched-rejected-20261108/README.md)
also failed their qualification gate: sampled Rust arms used up to two
broker sockets while librdkafka arms used one, and one Rust arm had no
independent readback after a sampler permission error. Nineteen arms were
verified. Their short, unmatched observations do not establish a ranking.

A [record-encoding experiment](../evidence/client/record-body-arithmetic-20261108/README.md)
was rejected after separate builds showed median elapsed time 6.1% higher
for an empty single-record batch and 4.0% higher for a keyed 100-byte batch.
The local shared-host codec test does not rank Kafka clients; an earlier
same-binary comparison was invalid and is retained in the report. Two
[one-byte varint candidates](../evidence/client/one-byte-varint-20261108/README.md)
also remain unapplied. In a balanced three-way x86 run, the outlined variant
improved the sampled header-heavy batch but was slower in all 12 paired
500-record plain and null-key batches. Inline results changed direction across
runs on different hosts and fixtures. The constant-input primitive loops can
allow compiler hoisting; these results do not rank Kafka implementations. Three
[batch-header candidates](../evidence/client/batch-header-write-20261108/README.md)
also remain unapplied: the stack version repeatedly slowed a warm single-record
case, and the reserve version slowed the sampled eight-record async case. A
third capacity-zero variant slowed an empty batch; its fresh-buffer gains do
not establish a benefit for the producer, which reserves the destination before
starting the header.

An isolated [transaction decode experiment](../evidence/client/txn-decode-20261108/README.md)
reduced loopback latency for larger responses on one shared x86 runner. It
is not applied: coverage, error precedence, cancellation and fairness need a
combined implementation and new measurements before any performance claim.

A [journal comparison](../evidence/broker/journal-release-cost-20261108/README.md)
measured the ownership repair against its baseline on one shared x86 host.
Open, append and staged-sync medians were higher in that workload; the write
samples varied widely and the host used an ext4 writeback mount with barriers
disabled. This does not qualify durable storage performance.
A [generation-lease variant](../evidence/broker/journal-generation-cost-20261108/README.md)
reduced ticket size but did not show a convincing timing gain and remains
unapplied.

The applied [record-header validation path](../evidence/broker/records-header-validation-20261108/README.md)
checks a validated prefix before resuming the generic walk. A seeded
differential corpus checks exact errors and partial counts. On one shared
x86 host, the fresh 13-case synthetic benchmark showed lower times for
header-heavy batches and a 0.3% higher median for 1,024 header-free records.
The default suite and a later all-feature candidate suite passed; an earlier
candidate suite and the same-runner baseline suite failed separate Raft tests.
Those failures remain unexplained. These measurements do not establish Kafka
throughput or a ranking.

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
external operator. The completed closed-loop run described above is
exploratory. Keep numbers limited to their settings, peers and architecture. The
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
