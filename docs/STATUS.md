# Status

Updated 3 November 2026.

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
  consumers and the publish dry run also pass at `fe65ac783`.
  See [package results][package].
- Broker epoch and journal changes now match the qualified x86 candidate: 720
  default and 868 all-feature tests passed with strict Clippy, docs and inventory
  checks. Earlier failures remain recorded. See [broker results][broker].
- Broker proposal admission now rejects an allocation whose capacity exceeds
  its configured command envelope. The [hosted capacity run][broker-capacity]
  reproduced the old failure and passed the fix in both feature profiles.
- Controller shutdown now retains an actor failure for later callers and
  preserves a pending join across cancellation. The [hosted candidate][broker-join-r2-result]
  reported a qualification pass; individual reports were inaccessible.
- Both focused capability profiles pass 93 required tests and 450 Java reverse
  checks. SDK socket and Streams Java coverage remain open. See
  [capability results][capability].
- The isolated [client v10 candidate][candidate-results] passed both library
  profiles and strict Clippy on its first recorded run. A [repeat][candidate-rerun]
  had one library failure in each profile. Both runs failed two integration binaries
  and one baseline expectation. Its changes have not been adopted on `main`;
  the fixture controls passed while the original timeout remained.
- The original main client failed the same 15 Mock Produce identities and the
  retained-backing test in a matching [thread and stack comparison][comparison].
  Fixture differences leave the shared causes unproven.

## Validation

| Check | Recorded result |
| --- | --- |
| Client default library on `main` at `e7660f6e5` | 2,168 passed, 28 failed; later integration targets did not run |
| Client all-feature library on `main` at that commit | 2,170 passed, 26 failed; later integration targets did not run |
| Isolated client v10 library, each profile | 2,280 passed initially; repeat 2,279 passed, one failed |
| Isolated client v14, 10 targets per profile | 2,431 passed, 17 failed and two ignored in each profile |
| Isolated client v16, 10 targets per profile | 2,432 passed, 16 failed and two ignored in each profile |
| Isolated client v16 repeat | Default: 2,432 passed, 16 failed, two ignored; all features: 2,431 passed, 17 failed, two ignored |
| Isolated client v17, each profile | 2,432 passed, 16 failed, two ignored; the retained-buffer timeout phase remains unknown |
| Qualified broker v2 default all targets, `209d0b068` | 720 passed |
| Qualified broker v2 all-feature all targets at that commit | 868 passed |
| Broker v2 strict Clippy, docs, inventory and six repeated Fetch checks | Passed |
| Client focused capability profiles at that commit | 93 passed and 450 Java reverse checks per profile |
| Main broker CI | 729 default and 877 all-feature tests passed at `2645e96ce`; Clippy, docs, inventory and fuzz supervision passed |
| Isolated broker capacity candidate, x86 | 729 default and 877 all-feature tests passed, with format, Clippy, docs and focused controls; OOM counters unavailable and zero doctests found |
| [Isolated broker shutdown candidate][broker-join-result] | Hosted qualification failed the inventory and inventory-negative-control gates |
| [Broker shutdown candidate r2][broker-join-r2-result] | Hosted qualification reported success; individual command reports and resource receipts were unavailable |
| Main capability profiles | Passed at `33744862c` |
| Main strict Clippy, security, documentation and package jobs | Passed at `e7660f6e5` |
| Main client CI | Failed at `e7660f6e5` |
| Live Kafka compatibility at `2645e96ce` | Kafka 4.1.2, 4.2.1 and 4.3.1 runs aborted with a stack overflow during classic group join |
| Windows default library at `2645e96ce` | 2,181 passed, 15 failed |

The [client failure index][client], [main CI run][main-ci] and
[candidate results][candidate-results] preserve earlier results. The
[latest client CI record][client-ci-2645] identifies the live compatibility
and Windows failures.
Earlier broker EOF, joined-shutdown QueueFull and Raft observer failures remain
in the [native-round record][native]. The source-bound inventory now passes all
55 controls. The [fixture gate repair][java] now has passing native results in
both profiles; two external SDK servers remain unrun. These focused passes do
not replace the complete current-commit release matrix.

## Open work

Producer worker retirement, shutdown and retry cancellation have 33 passing
focused controls. An earlier producer candidate library run had 2,176 passes and 20
failures; seven producer integration targets have 118 passes, 19 failures and
two ignored tests. These local runs used one test thread and an 8 MiB stack;
see [producer results][producer]. Partition issue ordering, modular sequence wrap, producer-identity
transitions and unresolved-batch replay still need repair and peer histories.

Consumer delivery, buffered offsets and aborted-transaction state must remain
consistent across errors, cancellation and topic recreation. Join/reset-leader
overlap, live group retries and a compatibility-test stack overflow also remain
open. GSSAPI and connection-driven SASL reauthentication
are incomplete; [protocol support](gaps.md) lists those feature gaps.
The [producer expiry review][sasl-expiry] also found a raw-write path without
an expiry check. It is source evidence; an expired request has not been
observed on a broker.

The [latest recorded main broker CI][broker-ci] passed after the bounded-read test fix.
Earlier runs reported `NotLeader`
during leader removal and in a separate observer test, plus two `ParseIntError`
failures. The broker
still needs replicated partition data, authenticated network
leadership, committed-watermark protection and transaction participant/marker
propagation. Controller queue memory accounting and a process-wide memory
bound remain open. The adopted local epoch and journal fixes have passing x86
qualification and ordinary broker CI on `b1b048809`.
Epoch publication rewrites the file and needs another complete image on disk.
A local log test does not establish a production cluster.

No 24-hour fault campaign or seven-day continuity run has passed. The retained
2,704-second epoch-recovery timeout is unexplained; successful seed replays
do not resolve it. See the [fault campaign record][faults].

Performance work is authorized on this shared x86_64 workspace. Matched
comparisons and independent record verification remain open. The
[benchmark rules](benchmark-contract.md) govern any claim. Current TLS uses
Ring, whose dependency includes C and assembly; an all-dependency Rust-only
claim is not established.

The producer's pending queue can repeatedly scan deadlines and shift records
when small batches drain. A replica epoch change rewrites the broker journal.
Both are candidates for optimization; neither has a measured end-to-end
speedup yet. The required matched x86 peer comparison has not run.
The [queue candidate][queue] needs passing native tests and a benchmark
before its effect is known; the [v17 benchmark prototype][queue-v17] remains
inactive until the baseline cancellation witness is repaired and the
same-job correctness gates pass. The [first combined candidate][candidate-build]
failed its test build because a test module omitted an import. The corrected
v16 candidate passed both library profiles and the revised deadline control,
but both profiles still failed 15 Mock Produce tests and one retained-buffer
test. A [repeat][candidate-v16-repeat] added one all-feature timing failure
and showed that baseline case 23 can match a coordinator error before its
cancellation barrier. The [v17 candidate][candidate-v17] retains that matcher;
its [hosted run][candidate-v17-result] still failed 15 Mock Produce tests and
one retained-buffer test per profile without locating the timeout. Its lane now
uses the historical `7e91e2b` checkout. A successor
draft remains unapplied after its final independent audit was stopped by an
automatic security review. See the [v16 results][candidate-v16] and
[next work][next] for details.

The [release scripts][release-gates] check API/index agreement, consumer lockfile
identity and exact-SHA CI more strictly. The token check reports presence without
claiming authorization. The CI checker now binds jobs and artifacts to a run
attempt, and 122 offline release tests passed after a temporary-path fix. Live
end-to-end attempt validation and the complete release gates remain open.

Use [next work][next] for the detailed handoff, [support](support.md) for tested
combinations, and the [release guide](RELEASE.md) for publication gates.
The [task registry][tasks] remains at 249 done, 13 in progress and 84 pending;
these changes close no cards. The [previous status][previous] retains older
measurements and dated notes.

[buffers]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/evidence/perf/buffer-reservations-20261030/README.md
[reconnect]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/evidence/client/reconnect-versions-20261030/README.md
[dependencies]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/evidence/security/dependency-floors-20261030/README.md
[ci]: https://github.com/mingley/partitionline/blob/c06dfd345315c1fd0847872a1176fde8c5959043/docs/evidence/releases/main-ci-fe65-20261030/README.md
[package]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/evidence/releases/package-navigation-20261030/README.md
[client]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/evidence/client/clean-default-validation-20261028/README.md
[main-ci]: https://github.com/mingley/partitionline/actions/runs/41576616173
[comparison]: https://github.com/mingley/partitionline/blob/f655b99c449b76ff09b30867ef58c42e51a38b65/docs/evidence/client/main-comparison-b1-20261103/README.md
[candidate-v16]: https://github.com/mingley/partitionline/blob/60212c74a31790a6c42052715b1d6e579d3cd961/docs/evidence/client/candidate-v16-job-20261103/REPORT.md
[candidate-v16-repeat]: https://github.com/mingley/partitionline/blob/5dfafef402abe292011cb6dac478dd89ab771277/docs/evidence/client/candidate-v16-repeat-e2a0-20261103/README.md
[sasl-expiry]: https://github.com/mingley/partitionline/blob/5dfafef402abe292011cb6dac478dd89ab771277/docs/evidence/client/sasl-expiry-boundary-20261103/README.md
[candidate-v17]: https://github.com/mingley/partitionline/blob/c06dfd345315c1fd0847872a1176fde8c5959043/docs/evidence/client/candidate-20261030/v17/README.md
[candidate-v17-result]: https://github.com/mingley/partitionline/blob/c340c4e7c0804700c42040f23accce5325a42d18/docs/evidence/client/candidate-v17-native-2645-20261103/README.md
[broker-join-result]: https://github.com/mingley/partitionline/blob/eae6a4538cb40528974ff0440db8be8730ada4ab/docs/evidence/broker/controller-join-native-664b-20261103/README.md
[broker-join-r2-result]: https://github.com/mingley/partitionline/blob/4e73f8774bcd7b62b08379eb3bfabc253cbc6c79/docs/evidence/broker/controller-join-native-r2-7e91-20261103/README.md
[broker-capacity]: https://github.com/mingley/partitionline/blob/c06dfd345315c1fd0847872a1176fde8c5959043/docs/evidence/broker/runtime-capacity-native-e2a0-20261103/README.md
[queue-v17]: https://github.com/mingley/partitionline/blob/c06dfd345315c1fd0847872a1176fde8c5959043/docs/evidence/perf/pending-queue-v17-20261103/README.md
[candidate-build]: https://github.com/mingley/partitionline/blob/f548870380ecc025b4e245c7f94a1c0a13fef2ff/docs/evidence/client/candidate-native-fdaf-20261103/README.md
[candidate-rerun]: https://github.com/mingley/partitionline/blob/2857611b35e25c9ad46206edae89c35e0bc0ffe9/docs/evidence/client/candidate-native-e766-20261103/README.md
[broker-ci]: https://github.com/mingley/partitionline/blob/466a87f242e8b043ae25d8f3a7ce2af7d1157559/docs/evidence/broker/main-ci-2645-20261103/REPORT.md
[client-ci-2645]: https://github.com/mingley/partitionline/blob/466a87f242e8b043ae25d8f3a7ce2af7d1157559/docs/evidence/client/main-ci-2645-20261103/REPORT.md
[release-gates]: https://github.com/mingley/partitionline/blob/929dd6bd20496f88a513a94baa644bc90f3d8eef/docs/evidence/releases/ci-attempt-binding-20261103/README.md
[candidate-results]: https://github.com/mingley/partitionline/blob/b2d18730494b6d55df7b81f9d4ce88a81e10b3d9/docs/evidence/client/candidate-native-b1-20261102/README.md
[java]: https://github.com/mingley/partitionline/blob/9481723c7a144254fdf4f1c415d082aa0233a25f/docs/evidence/client/capability-java-20261030/README.md
[broker]: https://github.com/mingley/partitionline/blob/ef28c255aecf402cdae2000d8796dcea9a36a8bc/docs/evidence/broker/epoch-journal-20261030/README.md
[capability]: https://github.com/mingley/partitionline/blob/ef28c255aecf402cdae2000d8796dcea9a36a8bc/docs/evidence/client/capability-native-20261030/README.md
[native]: https://github.com/mingley/partitionline/blob/dfad6573e46adf8aa91626854e4f816cfff2c2bf/docs/evidence/releases/candidate-native-20261030/README.md
[producer]: https://github.com/mingley/partitionline/blob/908dbdf2f20c8462a741ce59a4f56be2f310048e/docs/evidence/client/producer-admission-20261030/README.md
[faults]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/evidence/broker/fuzz/20261027-candidate06/README.md
[next]: https://github.com/mingley/partitionline/blob/4e73f8774bcd7b62b08379eb3bfabc253cbc6c79/docs/plan/next-work.md
[queue]: https://github.com/mingley/partitionline/blob/c06dfd345315c1fd0847872a1176fde8c5959043/docs/evidence/perf/producer-pending-20261102/README.md
[tasks]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/plan/tasks.json
[previous]: https://github.com/mingley/partitionline/blob/77021f7dc6c0d19aa93c7bf0b7fad125ee632e23/docs/STATUS.md
