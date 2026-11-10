# Next work

Updated 10 November 2026. Use latest stable Rust. The
[task registry](tasks.json) is the source of task status and dependencies;
the [task guide](README.md) explains how to select and record work.

## Qualification

The client and broker are not yet qualified for production, and neither has
an established fastest-in-the-world result. Client 0.1.0 is published; the
next client release must be at least 0.2.0. The experimental broker and schema
companion are unpublished.

The latest complete [main CI record](../evidence/releases/main-ci-7e6-20261110/README.md)
is for `7e6dff932db4128596ad45811f03c8c9d4afb4b7`. Linux client libraries
reported 2,313 passes and 15 failures with default features and 2,314 passes
and 14 failures with all features. Windows reported 2,308 passes and 20
failures; macOS reported 2,313 passes and 15 failures. All three current
Kafka compatibility jobs and both historical smoke jobs failed. The broker
default library passed, but a later default-feature target failed; its
all-feature and strict checks did not run. Package, docs and Schema jobs
passed. The finite resource-soak job passed; no sustained campaign has
passed. An [earlier snapshot](../evidence/releases/main-ci-669-20261109/README.md)
recorded an OS-denied process-descriptor sample; later passes do not explain it.

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

   A [cancellation review](../evidence/client/producer-assembly-drop-review-20261110/README.md)
   identifies records held in several owners during mixed-batch assembly.
   An isolated [count-reuse candidate](../evidence/client/producer-assembly-count-20261110/README.md)
   removes a redundant walk, but both baseline and candidate broad client
   profiles failed. The candidate remains unapplied and is not a delivery fix.
   A [worker-owned staging design](../evidence/client/producer-grouping-cancellation-design-20261110/README.md)
   covers the earlier drain boundary too. Its private-state cancellation control
   closed receipt senders and left a seeded byte charge at observation; no
   production fix or real Kafka delivery history is established.
   A [drain-staging prototype](../evidence/client/producer-drain-staging-20261110/README.md)
   restores records when the drain future is dropped. Focused controls passed,
   but its broader producer tests failed and its cost is not yet measured; it
   remains unapplied and does not cover the later grouping boundary.

2. **Client correctness and compatibility.** Work through the applicable
   failing client and Kafka profiles using the task registry. Preserve record
   ownership, ordering, deadlines and authentication across cancellation and
   reconnects. Keep mock, live-broker and independent peer results distinct.
   The [protocol gaps](../gaps.md) and [support matrix](../support.md) describe
   the current API and version boundaries. Offline fixtures cover selected
   Metadata [v13](../evidence/conformance/q-conf-metadatawire/README.md) and
   [v0–v12](../evidence/conformance/q-conf-metadatawire/all-versions-20261110.md)
   bodies against Apache SDKs. Version 0 is a raw wire case only. Wider error
   and collection cases, version negotiation and live-broker behavior remain open.

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
   observed quota sharing across two connections to Kafka 4.3.1. An isolated
   [client-label prototype](../evidence/broker/produce-client-quota-candidate-20261110/README.md)
   exercises the same wire scenario but uses static token buckets and remains
   unqualified. A related EOF refinement remains unapplied. The
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
   supported codecs, lifecycle and registry behavior before publication.
   Large-object duplicate-key detection now uses an indexed prefix; the
   [split-index evidence](../evidence/schema/json-avl-split-fastpath-20261110/README.md)
   records tests, large-object improvements and medium-object slowdowns.
   Parsing and allocation remain synchronous, and the body limit is not an RSS
   or time bound. Earlier and adaptive variants remain experiments.

5. **Performance.** Establish correctness before comparing throughput or
   latency. The isolated [indexed Fetch experiment](../evidence/broker/fetch-indexed-vectored-20261109/README.md)
   and [producer sampling-harness repair](../evidence/perf/kafka-ack-sampling-20261109/README.md)
   remain unapplied and need their recorded qualification gaps resolved. The
   [report validator](../evidence/perf/benchmark-report-validation-20261109/README.md)
   rejects several inconsistent record and settings fields. The [JSON report
   update](../evidence/perf/report-json-validation-20261110/README.md) rejects
   duplicate keys and labels alternate schemas, but cannot authenticate raw
   artifacts or establish a comparison. None of these results establishes a
   Kafka client or server ranking.

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
