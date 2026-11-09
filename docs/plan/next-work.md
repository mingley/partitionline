# Next work

Updated 9 November 2026. Use latest stable Rust. The
[task registry](tasks.json) is the source of task status and dependencies;
the [task guide](README.md) explains how to select and record work.

## Qualification

The client and broker are not yet qualified for production, and neither has
an established fastest-in-the-world result. Client 0.1.0 is published; the
next client release must be at least 0.2.0. The experimental broker and schema
companion are unpublished.

The latest complete [main CI record](../evidence/releases/main-ci-669-20261109/README.md)
is for `669f5b1e58103530f2a75020444d3148a9735f7e`. Linux client libraries
reported 2,312 passes and 16 failures with default features and 2,313 passes
and 15 failures with all features. Windows reported 2,306 passes and 22
failures; macOS reported 2,317 passes and 11 failures. All three current Kafka
compatibility jobs and both historical smoke jobs failed. The broker default
suite stopped after 580 passes, one failure and four ignored tests across the
targets reached; its all-feature and strict checks did not run. The
resource-soak driver failed before tests when the OS denied a `/proc` access;
no sustained campaign has passed.

Separate [combined-head checks](../evidence/ci/combined-a7b6-20261109/README.md)
at `a7b6edc1645e3a4789cf699a3b9e7656c55a3d10` passed focused Admin, producer
and broker transport tests in both feature profiles, strict client checks and
packed documentation checks. They do not replace the failing broad suites or
Kafka compatibility jobs. The [status](../STATUS.md) records other partial
runs and their limits.

## Priorities

1. **Producer delivery and lifecycle.** Use the
   [ownership integration checkpoint](../evidence/client/producer-ownership-integration-checkpoint-20261109/README.md)
   to coordinate admission, whole-batch retry, sequence and identity fencing,
   completion, flush and close. It is a source review, not an implementation.
   Existing [public ownership controls](../evidence/client/producer-ownership-regression-controls-20261108/README.md)
   use controlled mock scheduling and remain open. Preserve accepted records,
   receipts, callbacks and resource accounting across cancellation and worker
   replacement, then verify the resulting behavior against Kafka. An isolated
   [ownership attempt](../evidence/client/producer-bounded-ownership-attempt-20261109/README.md)
   passed selected cancellation controls but failed a retry-during-close
   control. An [admission-seal follow-up](../evidence/client/producer-admission-seal-v4-20261109/README.md)
   passed that mock but still lacks a fatal write fence. A
   [sealed-retry experiment](../evidence/client/producer-sealed-retry-experiment-20261109/README.md)
   lacks partition and identity fences. None is applied.

   The [close change](../evidence/client/producer-close-errors-20261109/README.md)
   returns the initial flush error after shutdown. Focused controls passed, but
   broad producer suites failed for both baseline and candidate; one additional
   all-feature candidate timing assertion also failed. A prior flush can consume
   an error, and successful close alone does not establish delivery.

2. **Client correctness and compatibility.** Work through the applicable
   failing client and Kafka profiles using the task registry. Preserve record
   ownership, ordering, deadlines and authentication across cancellation and
   reconnects. Keep mock, live-broker and independent peer results distinct.
   The [protocol gaps](../gaps.md) and [support matrix](../support.md) describe
   the current API and version boundaries.

   The [Admin cursor guard](../evidence/client/admin-cursor-history-20261109/README.md)
   catches some non-adjacent repeats with fixed-size history. It can reject a
   transient repeat that would later progress with a non-retriable protocol
   error; raw pages remain available. Focused checks passed, while broad Admin
   timing assertions failed on both baseline and candidate. Long comparisons
   and total result memory are not bounded by the history. The
   [Admin response diagnostic](../evidence/client/admin-response-growth-20261109/README.md)
   observed long map-growth polls in a synthetic debug fixture; it did not
   establish the cause of the earlier timing assertions.

3. **Broker durability and limits.** Complete and qualify partition replication,
   native Kafka KRaft, transaction marker propagation and resource bounds; the
   current broker has an RF1 data path and a separate metadata quorum. The
   applied [transport backpressure change](../evidence/broker/transport-backpressure-20261109/README.md)
   bounds read-ahead for valid pipelines, but a disconnect behind a full buffer
   can be observed later.

   Kafka Produce wire throttling remains unimplemented. The original
   [quota draft](../evidence/broker/produce-quota-draft-20261109/README.md)
   was not compiled because its workflows started no jobs. A newer
   [quota candidate](../evidence/broker/produce-quota-candidate-20261109/README.md)
   passed focused controls but failed its full all-feature suite; it remains
   unmerged. The
   [quota handoff plan](../evidence/broker/quota-handoff-20261109/README.md)
   describes the accounting, transport and authentication boundaries they
   need to satisfy.

4. **Schema companion.** The [Retry-After change](../evidence/schema/retry-after-20261109/README.md)
   handles delay-seconds for HTTP 429. The [incremental HTTP reader](../evidence/schema/registry-http-streaming-20261109/README.md)
   bounds decoded bodies and framing separately and avoids reparsing earlier
   chunks. Schema profiles and mock controls passed; no live registry was
   tested. Qualify the supported codecs, lifecycle and registry behavior before
   publication. The [JSON index](../evidence/schema/json-duplicate-index-20261109/README.md)
   and [compact follow-up](../evidence/schema/json-compact-index-20261109/README.md)
   remain unapplied after threshold slowdowns. [Balanced-tree experiments](../evidence/schema/json-balanced-index-20261109/README.md)
   also slowed ordinary Registry objects. An [incremental index](../evidence/schema/json-incremental-index-20261109/README.md)
   slowed a 96-key fixture and lacks a worst-case collision bound. Neither is
   applied.

5. **Performance.** Establish correctness before comparing throughput or
   latency. The isolated [indexed Fetch experiment](../evidence/broker/fetch-indexed-vectored-20261109/README.md)
   and [producer sampling-harness repair](../evidence/perf/kafka-ack-sampling-20261109/README.md)
   remain unapplied and need their recorded qualification gaps resolved.
   Neither establishes a Kafka client or server ranking.

## Validation and release

For each change, run the relevant focused controls, then the applicable
default and all-feature suites, formatting, strict lint, documentation,
package-consumer and independent compatibility checks on the exact source.
Retain failures, ignored tests and skipped work; a later focused pass does not
erase a broad-suite failure. Respect the stopped-work boundaries recorded in
[current status](../STATUS.md).

Performance qualification is scoped to x86_64. The
[benchmark contract](../benchmark-contract.md) requires matched settings,
independent record verification and retained evidence; x86 work does not
require another machine, architecture or operator. Shared-runner diagnostics
do not lift Suite HOLD or qualify a global speed claim. A 24-hour fault
campaign and seven-day continuity run remain open.

The [release guide](../RELEASE.md) requires exact-commit green CI, package
checks and an interruption-recovery rehearsal before publication. Do not
publish a new version until its applicable correctness and release gates pass.

The [previous detailed plan](https://github.com/mingley/partitionline/blob/7083355f7a8b4b4ac62499b65637760831f249f8/docs/plan/next-work.md)
preserves the earlier CI history, experiments, failures and evidence links.
