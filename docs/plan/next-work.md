# Next work

Updated 30 October 2026. Use latest stable Rust and push reviewed changes
directly to `main`. The [task registry](tasks.json) remains unchanged:
249 done, 13 in progress and 84 pending.

## Qualification state

Production readiness and fastest-client/server claims remain unqualified.
Client 0.1.0 is already published. The next client version must be at least
0.2.0; broker and schema publication remain disabled.

| Scope | Recorded result |
| --- | --- |
| Client default library, `77021f7dc` | 2,145 passed, 24 failed; later integration targets did not run |
| Client all-feature library at that commit | 2,152 passed, 17 failed; later integration targets did not run |
| Broker default all targets at that commit | 701 passed |
| Broker all features at that commit | 710 passed, two failed; stopped at `raft_runtime` |
| Client strict all-target Clippy, audit and dependency policy at that commit | Passed |
| Reconnect candidate | 14 unit controls and six integration targets passed; latest broad Fetch 167 passed, two failed, one ignored |
| Integrated-source package navigation | 20 documentation tests passed; extracted crate checked 30 documents, 225 local links, 51 pinned repository targets |
| Package consumers and publication dry run at `77021f7dc` | Passed; actual publication remains gated |
| New dependency floors | 14 metadata graphs and 11 resolver controls passed; GitHub audit and policy checks pass |
| Fault/continuity campaigns | No passing 24-hour or seven-day run |

The [client matrix](../evidence/client/clean-default-validation-20261028/README.md),
[CI snapshot](../evidence/releases/main-ci-77021-20261030/README.md),
[reconnect record](../evidence/client/reconnect-versions-20261030/README.md),
[package checks](../evidence/releases/package-navigation-20261030/README.md)
and [dependency record](../evidence/security/dependency-floors-20261030/README.md)
retain the exact scopes. Passing opt-in probes alone does not prove external
peer coverage. The broker workflow still failed its inventory and fuzz
supervisor setup. The previous commit's full broker passes remain in the
[earlier snapshot](../evidence/releases/main-ci-20261030/README.md).
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
   and five buffer-reservation fixes are prepared for default/all-feature
   qualification. A committed-watermark truncation guard and authenticated
   network integration remain open. Transaction checkpoints do not implement
   participant/marker propagation; marker and transactional-offset histories
   retain finite limits. See the
   [replication](../evidence/broker/replication-limits-20261028/README.md)
   and [transaction](../evidence/broker/transaction-checkpoint-20261028/README.md)
   records.

## Validation and performance

Run clean-commit client, broker and schema default/all-feature suites,
all-target strict Clippy, formatting, rustdoc, package consumers and independent
peer histories after fixes. Test-lint cleanup passes the remote client
all-target/all-feature gate at `77021f7dc`. Preserve all failed, skipped and ignored
cases. The [release guide](../RELEASE.md) requires exact-commit CI, package
checks and interruption-recovery rehearsal before publication.

The repaired capability inventory requires 93 named Rust cases and separately
reports two external server tests. Its 11 Python controls pass; both Rust lanes
still need to run. The Java codec setup now uses the LZ4 coordinate in Kafka's
published POM, with the existing jar hash. Missing cgroup quota metadata now
records an unknown value; the three pure controls pass, while actual worker
supervision remains unqualified. See the
[selection](../evidence/client/capability-selection-20261030/README.md) and
[CI prerequisite](../evidence/releases/ci-repairs-20261030/README.md) records.

The workspace has a four-CPU quota and limited disk. Serialize builds and
timing runs, retain source/executable hashes, and monitor a 1.5 GiB free-space
floor. Inactive Cargo cache files may be reclaimed only after exact compressed
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

The TLS graph still includes Ring C/assembly. The dependency update keeps that
provider. The passing audit applies to its checked lockfile and advisory
database revision; it does not establish an all-dependency Rust-only claim.
