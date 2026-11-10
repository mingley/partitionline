# Status

Updated 10 November 2026.

Partitionline is **not yet qualified for production use**. It has not
established that it is the fastest Kafka client or server.

The client provides producer, consumer, group, transaction and Admin APIs.
Version 0.1.0 is published; the repository contains later changes. The next
client release must be at least 0.2.0 because the public API and compiler
requirements have changed. The broker and Schema Registry companion are
unpublished. No new release is ready. The published
[0.1.0 overview](https://docs.rs/crate/partitionline/0.1.0) still contains
pre-publication installation instructions and older benchmark figures.

The broker is experimental. It has an RF1 data path and a separate metadata
quorum, but does not replicate partition data or implement native Kafka KRaft.
See the [support matrix](support.md) and [protocol gaps](gaps.md) for feature
coverage.

Build the current source with the latest stable Rust. The TLS dependency
graph includes Ring C and assembly, and broker OIDC also uses Ring directly.
The [provider assessment][tls] did not qualify a Rust-only alternative for
the current TLS and OIDC behavior.

## Validation

The latest complete recorded snapshot is main commit
`ffad9ef5d24f42cc5d8123288a8deffa8e4dc8c9`; the [CI record][ci]
links the individual runs and results. The current branch contains later changes.

| Check at `ffad9ef` | Result |
| --- | --- |
| Client library, Linux default | 2,312 passed, 16 failed; later targets did not run. |
| Client library, Linux all features | 2,305 passed, 23 failed; later targets did not run. |
| Client library, Windows default | 2,318 passed, 10 failed; later runtime and package steps did not all run. |
| Client library, macOS arm64 default | 2,310 passed, 18 failed; later runtime and package steps did not all run. |
| Kafka compatibility | 4.1.2, 4.2.1 and 4.3.1 profiles failed; 3.9.1 and 4.1.0 smoke jobs failed. |
| Resource-soak CI job | Passed its finite job; no 24-hour campaign completed. |
| Broker behavior and strict checks | Default and all-feature profiles, formatting, Clippy, rustdoc and doctests passed. Inventory and fuzz supervision also passed. |
| Client capability profiles | Both selected profiles passed; their archives were not independently inspected. |
| Client format, Clippy, audit, docs, package, auth and integrity smoke | Passed. Other jobs failed or were skipped. |
| Instruction-count gate | No baseline for this Rust version; no regression comparison. |

On the later [8570b01 client CI run][main-857-ci], the Schema job passed,
while the Linux and Windows client libraries and current Kafka jobs failed.
The docs and package jobs failed because of mutable links; the run was then
cancelled before its macOS job completed. The separate [broker run][broker-857-ci]
failed its default library. A [targeted documentation repair][docs-package]
passed the extracted-crate check. On [665fbd6 CI][main-665-ci], the repaired
package, docs and Schema jobs passed, but Kafka compatibility jobs failed and
other jobs were still running at this record. Full release gates remain open.

The [preceding complete snapshot][ci-92a] failed its broker default library;
the later pass does not explain that failure. An [earlier snapshot][ci-669]
recorded an OS-denied process-descriptor sample; later passes do not explain it.

The [combined-head checks][combined] passed formatting, strict client checks,
focused Admin, producer and broker transport tests in both feature profiles,
and packed-crate checks. The latter compiled 24 documented examples in each
of four feature configurations.

An earlier [partial CI record][ci-partial] on `0bed184` lists 13 successful
client jobs and nine failures; six jobs from the preceding full run are not
listed. Its broker behavioral job passed both profiles, but the broker and
capability workflows reported failure with jobs missing from the available
lists. Their causes are unknown.

An [earlier broker run][broker-earlier] on `ba893560` stopped in the default
library after 492 passes, one failure and four ignored. The cause is unknown;
the later broker pass does not explain it.

A focused [Schema Registry retry fix][schema-retry] now honors HTTP 429
`Retry-After` delay-seconds across the supported response framings. Its mock
controls and schema feature profiles passed; no live registry was tested. The
[incremental HTTP reader][schema-http] now bounds decoded bodies and framing
separately, enforces the header limit, and avoids reparsing prior chunks. Its
schema profiles and strict checks passed; live Registry and TLS interoperability
remain untested. A further [HTTP response fix][schema-responses] handles
informational and bodyless responses and rejects unsupported transfer codings;
its mock controls and all three schema profiles passed. The [large-object
JSON index][schema-json] reduces repeated key comparisons; its schema profiles
and finite differential controls passed. Parsing and allocation remain
synchronous, and the response body limit is not a time or RSS bound.

Earlier broker suites sometimes failed in integration or Raft tests. Later
passes do not establish their causes. The [previous detailed status][history]
and [work plan][next] retain those results and earlier evidence.

## Open work

- **Producer delivery.** Controlled mock histories expose sequence rollover,
  retry ordering and ownership problems. Accepted records can lose callbacks
  and leave buffer accounting nonzero after a worker stops. The
  [public controls][owner-controls] and [unwired batch draft][owner-draft]
  do not establish a fix or a real-Kafka outcome. The [ownership checkpoint][owner-checkpoint]
  maps the required admission, retry, completion and shutdown changes. In a
  later [instrumented mock][owner-baseline], 8,193 records had no callbacks
  and 65,544 bytes remained accounted for within a four-second observation
  after abort or timed close; this does not establish a permanent leak. An
  [ownership attempt][owner-attempt] passed selected cancellation controls
  but failed retry during close. An [admission-seal follow-up][owner-seal]
  passed that mock but lacks a fatal write fence; a separate
  [sealed-retry experiment][sealed-retry] lacks partition and identity fences.
  An isolated [offer-fence model][offer-model] passed 12 model controls but
  failed formatting; it depends on caller-attested transport retirement and
  has no restart safety. None is applied.
  [Seeded retry controls][retry-membership] also show that equal numeric bases
  can group records from different topics, partitions or identity generations;
  they do not establish a natural Kafka history. A
  [seeded rollback control][sequence-witness]
  showed a stale generation changing a current counter before a mock send;
  it did not establish a natural identity transition. A [guard experiment][guard-exp]
  passed selected controls but failed strict checks and does not fence writes
  across identity changes; it remains unapplied. The [close change][close-fix]
  now reports errors observed by its initial flush after shutdown; a prior
  flush can consume an error. Focused controls passed, but broader producer
  suites failed on baseline and candidate, with one additional candidate
  timing failure in the all-feature comparison. Repair ownership, sequence
  allocation, flush and shutdown together, then qualify delivery and
  transaction behavior against Kafka.
  A [mixed-batch cancellation review][assembly-review] identifies records held
  across several owners at a yield. A synthetic control saw closed receipts;
  it was not a public Kafka history. An isolated [count-reuse experiment][producer-count]
  does not fix delivery, and both baseline and candidate broad profiles failed.
  A [drain-to-grouping design][grouping-design] records a separate private
  control in which receipt senders closed and seeded bytes remained charged
  after cancellation; it does not establish a real Kafka history or a fix.
- **Client compatibility.** Current client library and Kafka jobs fail.
  Consumer and group behavior, cancellation, authentication and protocol
  coverage remain incomplete. An [Admin cursor guard][cursor-fix] detects
  some non-adjacent repeats with bounded history. It can reject a transient
  repeat that would later have progressed; raw page requests remain available.
  Focused checks passed, while broad Admin timing tests failed in both baseline
  and candidate. An [Admin response diagnostic][admin-growth] observed long
  map-growth polls in a synthetic debug fixture; it did not explain those
  earlier timing failures. Offline [Metadata v13][metadata-wire] and
  [v0–v12 fixtures][metadata-all] cover selected bodies against Apache SDKs;
  wider error and collection cases, negotiation and live-broker behavior remain
  open. Version 0 is a raw wire case only. The [protocol gaps](gaps.md)
  describe the APIs.
- **Broker durability and limits.** Partition replication, native KRaft,
  authenticated network leadership, transaction marker propagation and a
  process-wide memory bound remain open. Kafka wire throttling is not
  implemented; the [quota design][quota] and [handoff plan][quota-handoff]
  describe the remaining work. The initial [quota draft][quota-draft] was
  uncompiled and had an unbounded resume deadline. A newer [quota candidate][quota-candidate]
  passed focused controls but failed its full all-feature suite; it is not
  applied. A [live Kafka quota probe and idle-close control][quota-live]
  add scoped evidence. A separate [client-ID probe][quota-client-id] observed
  quota sharing across two connections to Kafka 4.3.1. A
  [client-label prototype][quota-named] exercises the same wire scenario using
  static token buckets. It passed focused checks but failed both broader
  broker profiles and remains unapplied. The related EOF
  refinement remains unapplied. The [transport fix][transport-fix] backpressures valid pipelines when read-ahead
  is full. A disconnect behind that buffer may be
  observed only after the active handler completes or reaches its deadline.
  Both full broker feature profiles and strict checks passed on its candidate.
- **Reliability.** No 24-hour fault campaign or seven-day continuity run has
  passed. Exact-commit CI, package checks and release rehearsal are still
  required by the [release guide](RELEASE.md).
- **Performance.** Matched x86 comparisons against other Kafka
  implementations with independent record verification remain incomplete.
  Scoped results, including the [Admin index comparison][admin-index], are
  not a client or server ranking. The [report validator][bench-validator]
  rejects several inconsistent settings and record histories. Its [JSON
  update][json-report] rejects duplicate keys and labels alternate schemas;
  it cannot verify raw artifacts or prove the declared settings were used. A
  [vectored Fetch experiment][fetch-exp]
  showed lower median time in several in-process workloads. Its Windows
  file-backed test failed before Fetch and strict checks remain incomplete;
  it is not applied. A
  [producer comparison harness repair][sampler-exp] also remains isolated
  pending integrated testing. Earlier [JSON index experiments][json-avl]
  and a [32/64-key comparison][json-avl-tail] remain unapplied. The current
  [split index][schema-json] speeds large synthetic objects but slowed some
  medium cases; it does not establish a general speedup or Kafka ranking.
  A [producer socket trace][cold-trace] measured the cost of an artificial
  handshake delay in a mock; no socket-reuse optimization is applied. A
  [Produce request encoding experiment][produce-plan-exp] improved several
  uncompressed fixtures but slowed LZ4 in all 12 pairs on one host. A
  [refinement][produce-plan-refined] also slowed empty and compressed cases in
  several pairs on a second host. Neither is applied or measures the ordinary
  producer. The
  [benchmark contract](benchmark-contract.md) sets out the measurement rules.

An automatic security review stopped specific segmented-log recovery and
consumer/group investigations, stopped an independent v18 audit, and rejected
an earlier ReadCommitted rollback validation. Those specific tasks remain
stopped.

The [work plan][next] and its linked evidence track the outstanding work.
The [previous detailed status][history] preserves earlier qualification
results and experiments.

[ci]: https://github.com/mingley/partitionline/blob/a2d0af0a71a1364fefd3c2520dca4b7041d4a2dc/docs/evidence/releases/main-ci-ffad-20261110/README.md
[history]: https://github.com/mingley/partitionline/blob/dfe179d7066e2c1afecb88df5ba76d2920308c46/docs/STATUS.md
[next]: https://github.com/mingley/partitionline/blob/8913f1a00a3b770bba7eb64eff836274e35c23b4/docs/plan/next-work.md
[tls]: https://github.com/mingley/partitionline/blob/e18186f0cb9c25393117fe41148ae5860f52a8a0/docs/evidence/security/tls-provider-assessment-20261108/README.md
[owner-controls]: https://github.com/mingley/partitionline/blob/e18186f0cb9c25393117fe41148ae5860f52a8a0/docs/evidence/client/producer-ownership-regression-controls-20261108/README.md
[owner-draft]: https://github.com/mingley/partitionline/blob/ed86c9c937dd3f5a7edb73858a6474b04073a827/docs/evidence/client/producer-owned-batch-rust-draft-20261108/README.md
[quota]: https://github.com/mingley/partitionline/blob/ed86c9c937dd3f5a7edb73858a6474b04073a827/docs/evidence/broker/quota-produce-design-20261108/README.md
[admin-index]: https://github.com/mingley/partitionline/blob/89a8e2e5ae2ab28942be66c8bd7e0390032b4450/docs/evidence/client/admin-dtp-slot-index-20261108/README.md
[schema-retry]: https://github.com/mingley/partitionline/blob/ef20cb151dc7d310ab6f16d72b82ebc6c1071574/docs/evidence/schema/retry-after-20261109/README.md
[fetch-exp]: https://github.com/mingley/partitionline/blob/b0ae69f703bc1cec5ae3d8ce860035f08a37cb93/docs/evidence/broker/fetch-indexed-vectored-20261109/README.md
[sampler-exp]: https://github.com/mingley/partitionline/blob/b0ae69f703bc1cec5ae3d8ce860035f08a37cb93/docs/evidence/perf/kafka-ack-sampling-20261109/README.md
[close-fix]: https://github.com/mingley/partitionline/blob/d3bed8c7f61d541899c455b62595fff4650d4b9c/docs/evidence/client/producer-close-errors-20261109/README.md
[cursor-fix]: https://github.com/mingley/partitionline/blob/d3bed8c7f61d541899c455b62595fff4650d4b9c/docs/evidence/client/admin-cursor-history-20261109/README.md
[transport-fix]: https://github.com/mingley/partitionline/blob/d3bed8c7f61d541899c455b62595fff4650d4b9c/docs/evidence/broker/transport-backpressure-20261109/README.md
[json-index]: https://github.com/mingley/partitionline/blob/d3bed8c7f61d541899c455b62595fff4650d4b9c/docs/evidence/schema/json-duplicate-index-20261109/README.md
[combined]: https://github.com/mingley/partitionline/blob/2f5f6b2742e15aa6d22a89af4b3cd81bb34c0e09/docs/evidence/ci/combined-a7b6-20261109/README.md
[broker-earlier]: https://github.com/mingley/partitionline/actions/runs/42667809972
[quota-handoff]: https://github.com/mingley/partitionline/blob/2f5f6b2742e15aa6d22a89af4b3cd81bb34c0e09/docs/evidence/broker/quota-handoff-20261109/README.md
[json-compact]: https://github.com/mingley/partitionline/blob/2f5f6b2742e15aa6d22a89af4b3cd81bb34c0e09/docs/evidence/schema/json-compact-index-20261109/README.md
[sequence-witness]: https://github.com/mingley/partitionline/blob/5c6188deff80c8d29295f5b02491877165e97c98/docs/evidence/client/producer-stale-rollback-20261109/README.md
[quota-draft]: https://github.com/mingley/partitionline/blob/5c6188deff80c8d29295f5b02491877165e97c98/docs/evidence/broker/produce-quota-draft-20261109/README.md
[cold-trace]: https://github.com/mingley/partitionline/blob/5c6188deff80c8d29295f5b02491877165e97c98/docs/evidence/client/producer-cold-handshake-20261109/README.md
[ci-partial]: https://github.com/mingley/partitionline/blob/191df95de76ac2b35a061ce205f47d94c0a6a9c5/docs/evidence/releases/main-ci-0bed-partial-20261109/README.md
[guard-exp]: https://github.com/mingley/partitionline/blob/191df95de76ac2b35a061ce205f47d94c0a6a9c5/docs/evidence/client/producer-sequence-guard-20261109/README.md
[owner-checkpoint]: https://github.com/mingley/partitionline/blob/65830a2f7bf6afaadc9d287665e44592ef24d72f/docs/evidence/client/producer-ownership-integration-checkpoint-20261109/README.md

[owner-baseline]: https://github.com/mingley/partitionline/blob/3c29521973166b64b15fc498e7e47c4ec0eebeaa/docs/evidence/client/producer-ownership-baseline-731eb97/README.md
[produce-plan-exp]: https://github.com/mingley/partitionline/blob/a15794155397da3842e16c5b7703e615b7be4676/docs/evidence/client/produce-request-first-length-20261109/README.md
[produce-plan-refined]: https://github.com/mingley/partitionline/blob/af33269d4c35c84daa6a1395558ed03f73893cd6/docs/evidence/client/produce-request-uncompressed-length-20261109/README.md
[schema-http]: https://github.com/mingley/partitionline/blob/172ce91a2c279446caa8de7ac0fd2d6c9e9304ac/docs/evidence/schema/registry-http-streaming-20261109/README.md
[retry-membership]: https://github.com/mingley/partitionline/blob/2e039416e19d049aee12dbff1e442f26d0088dd2/docs/evidence/client/producer-retry-membership-20261109/README.md
[owner-attempt]: https://github.com/mingley/partitionline/blob/ad7d671c7cbfdc87dbf1c797b02c5ea9890f1808/docs/evidence/client/producer-bounded-ownership-attempt-20261109/README.md
[sealed-retry]: https://github.com/mingley/partitionline/blob/ad7d671c7cbfdc87dbf1c797b02c5ea9890f1808/docs/evidence/client/producer-sealed-retry-experiment-20261109/README.md
[quota-candidate]: https://github.com/mingley/partitionline/blob/ad7d671c7cbfdc87dbf1c797b02c5ea9890f1808/docs/evidence/broker/produce-quota-candidate-20261109/README.md
[json-balanced]: https://github.com/mingley/partitionline/blob/ad7d671c7cbfdc87dbf1c797b02c5ea9890f1808/docs/evidence/schema/json-balanced-index-20261109/README.md
[json-incremental]: https://github.com/mingley/partitionline/blob/ad7d671c7cbfdc87dbf1c797b02c5ea9890f1808/docs/evidence/schema/json-incremental-index-20261109/README.md
[ci-previous]: https://github.com/mingley/partitionline/blob/b78c9cfd89bd02a35ac04040ba9be43041cb0364/docs/evidence/releases/main-ci-f1be-20261109/README.md
[admin-growth]: https://github.com/mingley/partitionline/blob/658620ca4681aefe208aab9aa486ecb7a4afe89e/docs/evidence/client/admin-response-growth-20261109/README.md
[owner-seal]: https://github.com/mingley/partitionline/blob/ce488cd1d71febefd1dd9abff9b3ba774443e73e/docs/evidence/client/producer-admission-seal-v4-20261109/README.md
[ci-71]: https://github.com/mingley/partitionline/blob/7cfe6afd22d4099e602365e8a71428913eecf567/docs/evidence/releases/main-ci-71c-20261109/README.md

[offer-model]: https://github.com/mingley/partitionline/blob/f7c5d78d49333a754699af1cbb10fcaba1bcce74/docs/evidence/client/producer-offer-fence-model-20261109/README.md

[bench-validator]: https://github.com/mingley/partitionline/blob/4664aa3fc47f719a726e049535bdf48b8453094b/docs/evidence/perf/benchmark-report-validation-20261109/README.md

[schema-responses]: https://github.com/mingley/partitionline/blob/b54eda17ca32a200345a983b76d063be76c7b048/docs/evidence/schema/http-response-semantics-20261109/README.md

[quota-live]: https://github.com/mingley/partitionline/blob/ad0e8944dcd5e5a704e1a1d5439aa3e0d84a43f4/docs/evidence/broker/produce-quota-live-idle-20261109/README.md

[ci-669]: https://github.com/mingley/partitionline/blob/7cfe6afd22d4099e602365e8a71428913eecf567/docs/evidence/releases/main-ci-669-20261109/README.md

[ci-fd4]: https://github.com/mingley/partitionline/blob/88a6dc53222445aa611b4b782ed832aed433ed99/docs/evidence/releases/main-ci-fd4-20261109/README.md
[metadata-wire]: https://github.com/mingley/partitionline/blob/2463421e6137185a506c71fdb0fe4acaee530cab/docs/evidence/conformance/q-conf-metadatawire/README.md

[ci-199]: https://github.com/mingley/partitionline/blob/2463421e6137185a506c71fdb0fe4acaee530cab/docs/evidence/releases/main-ci-199-20261109/README.md
[metadata-all]: https://github.com/mingley/partitionline/blob/a2d0af0a71a1364fefd3c2520dca4b7041d4a2dc/docs/evidence/conformance/q-conf-metadatawire/all-versions-20261110.md
[metadata-boundaries]: https://github.com/mingley/partitionline/blob/2ac5db7947ab9b13b3e0139df9a7c22f30ead38d/docs/evidence/conformance/q-conf-metadatawire/boundaries-20261110.md
[quota-client-id]: https://github.com/mingley/partitionline/blob/2ac5db7947ab9b13b3e0139df9a7c22f30ead38d/docs/evidence/broker/produce-client-quota-isolation-20261110/README.md
[json-avl]: https://github.com/mingley/partitionline/blob/cfe9a1ac5187aebad36269681275af97bf68ba60/docs/evidence/schema/json-progressive-avl-20261110/README.md

[quota-named]: https://github.com/mingley/partitionline/blob/dab49628133bb7056f761bcb2ab8c1e12ba9250e/docs/evidence/broker/produce-client-quota-candidate-20261110/README.md
[json-avl-tail]: https://github.com/mingley/partitionline/blob/cfe9a1ac5187aebad36269681275af97bf68ba60/docs/evidence/schema/json-avl-tail-comparison-20261110/README.md

[ci-92a]: https://github.com/mingley/partitionline/blob/2ac5db7947ab9b13b3e0139df9a7c22f30ead38d/docs/evidence/releases/main-ci-92a-20261110/README.md
[schema-json]: https://github.com/mingley/partitionline/blob/a2d0af0a71a1364fefd3c2520dca4b7041d4a2dc/docs/evidence/schema/json-avl-split-fastpath-20261110/README.md
[producer-count]: https://github.com/mingley/partitionline/blob/a2d0af0a71a1364fefd3c2520dca4b7041d4a2dc/docs/evidence/client/producer-assembly-count-20261110/README.md
[assembly-review]: https://github.com/mingley/partitionline/blob/a2d0af0a71a1364fefd3c2520dca4b7041d4a2dc/docs/evidence/client/producer-assembly-drop-review-20261110/README.md

[main-857-ci]: https://github.com/mingley/partitionline/actions/runs/42878680393
[docs-package]: https://github.com/mingley/partitionline/blob/fc839cf08e6bdcadf1e4f022b570d4cb53637176/docs/evidence/releases/docs-package-repair-20261110/README.md
[grouping-design]: https://github.com/mingley/partitionline/blob/fc839cf08e6bdcadf1e4f022b570d4cb53637176/docs/evidence/client/producer-grouping-cancellation-design-20261110/README.md

[broker-857-ci]: https://github.com/mingley/partitionline/actions/runs/42878680382
[main-665-ci]: https://github.com/mingley/partitionline/actions/runs/42881952564
[json-report]: https://github.com/mingley/partitionline/blob/8913f1a00a3b770bba7eb64eff836274e35c23b4/docs/evidence/perf/report-json-validation-20261110/README.md
