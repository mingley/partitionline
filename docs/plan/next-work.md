# Next work

Updated 10 November 2026. Use latest stable Rust. The
[task registry](tasks.json) is the source of task status and dependencies;
the [task guide](README.md) explains how to select and record work.

## Qualification

The client and broker are not yet qualified for production, and neither has
an established fastest-in-the-world result. Client 0.1.0 is published; the
next client release must be at least 0.2.0. The experimental broker and schema
companion are unpublished.

The latest complete [main CI record](../evidence/releases/main-ci-92a-20261110/README.md)
is for `92a365f2829c00438b6c58841dbb0990fca480d6`. Linux client libraries reported 2,314 passes and 14
failures with default features and 2,313 passes and 15 failures with all
features. Windows reported 2,303 passes and 25 failures; macOS reported 2,317
passes and 11 failures. All three current Kafka compatibility jobs and both
historical smoke jobs failed. The broker default library stopped after 495
passes, one failure and four ignored; later broker checks did not run. The
finite resource-soak CI job passed; no sustained campaign has passed. The
[preceding snapshot](../evidence/releases/main-ci-199-20261109/README.md)
passed broker checks, but does not explain the failure. An
[earlier 669 snapshot](../evidence/releases/main-ci-669-20261109/README.md)
recorded an OS-denied `/proc` access; later passes do not explain it.

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
   lacks partition and identity fences. An isolated
   [offer and identity fence model](../evidence/client/producer-offer-fence-model-20261109/README.md)
   passed 12 model controls but failed formatting and is not wired to the
   producer. Its transport retirement is caller-attested and it has no restart
   safety. None is applied.

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
   the current API and version boundaries. An offline [Metadata v13 fixture](../evidence/conformance/q-conf-metadatawire/README.md)
   and [boundary fixture](../evidence/conformance/q-conf-metadatawire/boundaries-20261110.md)
   cover selected bodies against Apache SDKs. Versions 2 and 4–7, wider error
   cases, version negotiation and live-broker behavior remain open.

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
   unmerged. A [live Kafka timing probe and idle-close control](../evidence/broker/produce-quota-live-idle-20261109/README.md)
   add scoped evidence. A [client-ID probe](../evidence/broker/produce-client-quota-isolation-20261110/README.md)
   observed quota sharing across two connections to Kafka 4.3.1. A related EOF
   refinement passed focused controls but failed formatting and remains
   unapplied. The
   [quota handoff plan](../evidence/broker/quota-handoff-20261109/README.md)
   describes the accounting, transport and authentication boundaries they
   need to satisfy.

4. **Schema companion.** The [Retry-After change](../evidence/schema/retry-after-20261109/README.md)
   handles delay-seconds for HTTP 429. The [incremental HTTP reader](../evidence/schema/registry-http-streaming-20261109/README.md)
   bounds decoded bodies and framing separately and avoids reparsing earlier
   chunks. Schema profiles and mock controls passed; no live registry was
   tested. The [HTTP response handling change](../evidence/schema/http-response-semantics-20261109/README.md)
   handles informational and bodyless responses and rejects unsupported transfer
   codings; mock controls and all three schema profiles passed. Qualify the
   supported codecs, lifecycle and registry behavior before publication. The [JSON index](../evidence/schema/json-duplicate-index-20261109/README.md)
   and [compact follow-up](../evidence/schema/json-compact-index-20261109/README.md)
   remain unapplied after threshold slowdowns. [Balanced-tree experiments](../evidence/schema/json-balanced-index-20261109/README.md)
   also slowed ordinary Registry objects. An [incremental index](../evidence/schema/json-incremental-index-20261109/README.md)
   slowed a 96-key fixture and lacks a worst-case collision bound. A
   [progressive AVL experiment](../evidence/schema/json-progressive-avl-20261110/README.md)
   improved large objects but slowed ordinary fixtures on one runner. None is
   applied.

5. **Performance.** Establish correctness before comparing throughput or
   latency. The isolated [indexed Fetch experiment](../evidence/broker/fetch-indexed-vectored-20261109/README.md)
   and [producer sampling-harness repair](../evidence/perf/kafka-ack-sampling-20261109/README.md)
   remain unapplied and need their recorded qualification gaps resolved. The
   [report validator](../evidence/perf/benchmark-report-validation-20261109/README.md)
   now rejects several inconsistent record and settings fields, but cannot
   verify the raw artifacts or establish a comparison. None of these results
   establishes a Kafka client or server ranking.

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
