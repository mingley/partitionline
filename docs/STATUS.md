# Status

Updated 30 October 2026.

`partitionline` 0.1.0 is published on crates.io. This repository contains later
changes and requires the latest stable Rust. The next client release must be
at least 0.2.0 because of public API and compiler-requirement changes. The broker
and schema companion remain unpublished.

Production readiness and fastest-client/server claims are unqualified.
The client implements produce, fetch, groups, transactions and Admin. The broker
is experimental: its operator has an RF1 data path and a separate metadata
quorum, without replicated partition data or native Kafka KRaft.

## Recent changes

- Buffer growth fixes restored the codec allocation gate without raising its
  budgets. A 48-cell encoder census dropped from 84 growth allocations to 48,
  with identical bodies and zero warmed allocations. This was an allocation
  comparison, not a throughput measurement. See [buffer results][buffers].
- Replacement sockets negotiate and authenticate independently. Offset retries
  re-encode for the destination's ranges or reject fields that cannot be
  represented. Fourteen focused unit controls and six integration targets
  passed. The latest broad Fetch run reported 167 passed, two failed and one
  ignored; earlier timing and timeout failures remain recorded. See
  [reconnect results][reconnect].
- Dependency minimums now exclude the versions named by the recorded TLS,
  certificate, Tokio and RustCrypto advisories. Fourteen metadata graphs and
  eleven resolver controls passed. The subsequent GitHub audit and dependency
  policy checks also pass. New runtime qualification remains open. See
  [dependency results][dependencies] and the [CI snapshot][ci].
- The extracted crate passes documentation navigation, license and exclusion
  checks: 30 documents, 225 local links and 51 pinned repository targets.
  All 20 documentation tests passed. GitHub documentation consumers, package
  consumers and the publish dry run also pass at `77021f7dc`.
  See [package results][package].

## Validation

| Check | Recorded result |
| --- | --- |
| Client default library at `77021f7dc` | 2,145 passed, 24 failed; later integration targets did not run |
| Client all-feature library at that commit | 2,152 passed, 17 failed; later integration targets did not run |
| Broker default all targets at that commit | 701 passed |
| Broker all features at that commit | 710 passed, two failed; stopped at `raft_runtime` |
| Client strict all-target Clippy, security, documentation and package jobs | Passed at that commit |
| Exact current-commit CI and release gates | Not passed |

The [client failure index][client] and [CI snapshot][ci] preserve the results.
The broker workflow also failed its source-pinned inventory and fuzz supervisor
setup. Both capability lanes have stale case counts; the repaired selection
requires 93 named Rust cases and reports two external servers separately.
Successful opt-in probes do not establish external peer coverage. Later focused
results do not replace a clean full matrix.

## Open work

Producer worker retirement, shutdown and retry cancellation have 33 passing
focused controls. The broader candidate library run has 2,176 passes and 20
failures; seven producer integration targets have 118 passes, 19 failures and
two ignored tests. These local runs used one test thread and an 8 MiB stack;
see [producer results][producer]. Partition issue ordering, modular sequence wrap, producer-identity
transitions and unresolved-batch replay still need repair and peer histories.

Consumer delivery, buffered offsets and aborted-transaction state must remain
consistent across errors, cancellation and topic recreation. Join/reset-leader
overlap, live group retries and a compatibility-test stack overflow also remain
open. GSSAPI and connection-driven SASL reauthentication
are incomplete; [protocol support](gaps.md) lists those feature gaps.

The broker still needs replicated partition data, authenticated network
leadership, committed-watermark protection and transaction participant/marker
propagation. Local replica-epoch and journal-publication fixes are under
qualification. A local log test does not establish a production cluster.

No 24-hour fault campaign or seven-day continuity run has passed. The retained
2,704-second epoch-recovery timeout is unexplained; successful seed replays
do not resolve it. See the [fault campaign record][faults].

Performance work is authorized on this shared x86_64 workspace. Matched
comparisons and independent record verification remain open. The
[benchmark rules](benchmark-contract.md) govern any claim. Current TLS uses
Ring, whose dependency includes C and assembly; an all-dependency Rust-only
claim is not established.

Use [next work][next] for the detailed handoff, [support](support.md) for tested
combinations, and the [release guide](RELEASE.md) for publication gates.
The [task registry][tasks] remains at 249 done, 13 in progress and 84 pending;
these changes close no cards. The [previous status][previous] retains older
measurements and dated notes.

[buffers]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/evidence/perf/buffer-reservations-20261030/README.md
[reconnect]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/evidence/client/reconnect-versions-20261030/README.md
[dependencies]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/evidence/security/dependency-floors-20261030/README.md
[package]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/evidence/releases/package-navigation-20261030/README.md
[client]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/evidence/client/clean-default-validation-20261028/README.md
[ci]: https://github.com/mingley/partitionline/blob/908dbdf2f20c8462a741ce59a4f56be2f310048e/docs/evidence/releases/main-ci-77021-20261030/README.md
[producer]: https://github.com/mingley/partitionline/blob/908dbdf2f20c8462a741ce59a4f56be2f310048e/docs/evidence/client/producer-admission-20261030/README.md
[faults]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/evidence/broker/fuzz/20261027-candidate06/README.md
[next]: https://github.com/mingley/partitionline/blob/908dbdf2f20c8462a741ce59a4f56be2f310048e/docs/plan/next-work.md
[tasks]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/plan/tasks.json
[previous]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/STATUS.md
