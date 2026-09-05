# Plan for a best-in-class Kafka client

The goal is to make `partitionline` a leading choice for Kafka applications:
correct under failure, predictable under load, efficient at equivalent
semantics, and straightforward to operate. "Best" must mean a reproducible
result for a named workload, not a universal claim.

This is an execution plan, not a certification or permission to deploy.
[TODO.md](../TODO.md) tracks the work. Numerical targets below are **proposed**,
not achieved guarantees. Michael Ingley is the coordinating maintainer and
scope/signoff DRI; implementers and independent reviewers are unassigned.

## 1. Resume from evidence, not from assumed completion

Baseline: [54020e2](https://github.com/mingley/partitionline/tree/54020e2f9e695b6d1c817cc80ec9e99e4e171df9),
reviewed 2026-09-04. Earlier work stopped mid-flight, leaving code and notes
frozen. Reconcile [STATUS.md](STATUS.md), [CIVILIZATION.md](CIVILIZATION.md),
[gaps.md](gaps.md) and the current source before carrying forward a "done" label.
Distinguish shipped behavior, draft proposals, test coverage and actual results.
An interrupted handoff does not mean every component is broken.

**Honesty bar:** crates.io `0.1.0` is already Installable, with adopter pins and
the post-cut work on `main`. **Suite HOLD remains** until signed Lab A evidence.
Unsigned Verifiable/latency samples do not lift it; see
[CIVILIZATION.md](CIVILIZATION.md) and [STATUS.md](STATUS.md).

One concrete baseline failure is
[CI run 33938039612](https://github.com/mingley/partitionline/actions/runs/33938039612):
`fmt` rejected import ordering/spacing in `tests/fuzz_decode_smoke.rs`.
The `integrity-smoke` job's count checks passed (`acked=2000`,
`hw_delta=2000`, `consumed=2000`), then its latency gate failed:
produce-ack p99 was **1,344 us**, above the **750 us** ceiling (500 us baseline
plus 50% slack). This is not evidence of data loss, nor do matching counts alone
prove exactly-once behavior. The latency failure's cause is not established.

The run started native Kafka 4.1.0; a Docker 3.9.1 attempt conflicted on port
9092 before falling back to native tools. Record the broker actually exercised,
not just the requested image. The separate CI latency job has a different
absolute ceiling; reconcile those policies rather than blindly relaxing either.
Release-plz succeeded independently, so publication is not evidence of green CI.

**Integration update:** incoming `main` through `0146b98` was preserved when
landing this plan. It includes the
[formatting and CI-policy repair](https://github.com/mingley/partitionline/commit/e38e5ce):
the nested latency gate is now skipped in the integrity CI job while the
dedicated latency job remains. Do not schedule those source fixes again.
Controlled-host latency qualification and verification of current required lanes
remain work; the old failing run is historical evidence, not a claim that the
same jobs still fail at the integrated HEAD.

| Area | Existing evidence and source | Qualification gap |
|---|---|---|
| Package and dependency boundary | [Cargo.toml](../Cargo.toml): published `partitionline` 0.1.0, Rust 1.85 declaration, MIT OR Apache-2.0. Client code forbids unsafe; no librdkafka dependency. TLS uses `rustls`/`ring`, including native compilation via `cc`. | Published/installable does not mean production-qualified. Do not claim the full build has no C dependencies. |
| Producer, consumer and protocol | [producer](../src/producer.rs), [consumer](../src/consumer.rs), [protocol](../src/protocol), [mock tests](../tests/full_surface.rs): routing, negotiated versions, batching, retries, idempotence and transactions. | Version support needs per-API evidence; mock agreement is not an independent broker oracle. |
| Group and share APIs | [group](../src/group.rs), [share](../src/share.rs), [broker smoke](../scripts/ci-broker-smoke.sh): classic/cooperative, KIP-848 and share-group paths. | Existing 3.9.1/4.1.0 CI smoke is not multi-broker chaos. Check executed cases and capability gates; an ignored live test is not covered by default `cargo test`. |
| Build and safety CI | [CI](../.github/workflows/ci.yml) already includes Rust 1.85/stable, features, package, audit/deny, short fuzz, broker and auth lanes. | Repair failures and verify required lanes cannot silently skip/fallback to a different advertised matrix cell. Extend existing lanes instead of creating duplicates. |
| Codecs and ecosystem | [zstd spike](zstd-spike.md), [schema companion](schema-companion.md): gzip/snappy/LZ4 exist; zstd is not implemented; Schema Registry is a companion design. | Evaluate zstd decoding and encoding separately. Do not assume `ruzstd` supplies a compressor Keep zstd **out of defaults** (`docs/zstd-spike.md`); treat opt-in C (`zstd-sys`) only if survey #85 demands it — never as a default-feature P0. or that a companion proposal is a published crate. |
| Performance | [benchmark.md](benchmark.md) separates locked Lab A produce results from unsigned this-VM fetch/latency results. Recorded produce latency was 62/95 us p50/p99 versus rust-rdkafka 58/90 us. | No universal speed claim. Preserve Suite HOLD and its signoff rules; do not combine different hosts/configurations into one victory. |
| Operations | [metrics](../src/metrics.rs), optional `tracing`, [security policy](security.md), [adoption checklist](ADOPTION.md) already exist. | Prove diagnostic usefulness, redaction, auth rotation, recovery and operator-driven rollback on a defined profile. |

## 2. Scope and promotion gates

| Profile | Required work | Not automatically required |
|---|---|---|
| Bounded producer/consumer deployment | KL-01/02, applicable leader-recovery cases in KL-03, transport/security in KL-06, diagnostics in KL-07, baseline measurements in KL-04 and release/adoption in KL-08. | zstd, Schema Registry, Kerberos, every Kafka API or a benchmark victory. |
| Transactional processing | Above plus KL-03 crash histories, fencing, aborted-record visibility and atomic output/offset proofs. | Exactly-once external side effects without application cooperation. |
| Dynamic group or share-group deployment | Above plus the corresponding KL-03 broker-version/churn/lock-recovery matrix. | Treating a classic-group success as evidence for KIP-848 or share semantics. |
| Wider ecosystem support | KL-05 and explicit demand-driven designs for missing capabilities. | Cloning the librdkafka API or adding codec/schema/exporter dependencies to the default graph without a decision. |

Production qualification is profile-specific. Semver 1.0 additionally requires
a stable API, supported-version/MSRV policy and a maintenance commitment.
It does not require every optional feature or lifting a performance claim HOLD.
Keep [api-stability.md](api-stability.md) and the support matrix authoritative.

## 3. Scorecard

Freeze workload, hardware, broker settings and thresholds before each
qualification exercise. Target changes need a recorded rationale; never adjust
them retrospectively merely to make a failing result green.

| Dimension | Proposed acceptance target | Evidence |
|---|---|---|
| Integrity | Zero unexplained missing/corrupt records, forbidden duplicates, order violations or exposure of aborted transactions. | Unique-ID/payload histories, outcome classification and independent consumers; not count equality alone. |
| Compatibility | Zero unexplained semantic differences across the advertised negotiated API/version matrix. | Pinned Java/broker oracle, golden wire fixtures and minimized differential failures; allow legitimate wire/layout differences. |
| Bounded resources | A declared byte/task/connection budget holds at 2x sustainable offered load. After warmup, final-hour median RSS within 10% of first steady-state hour over 24 hours. | Buffer ownership/counter evidence and RSS; allocator retention is not automatically a leak, and queue bounds alone are not a process bound. |
| Recovery | Initial target: resume eligible work within 5 seconds after a controlled broker restoration; respect earlier caller deadlines. | At least 1,000 seeded faults per supported profile. Group timers/transaction timeouts get separate predeclared budgets, not a universal 500 ms promise. |
| Performance leadership | At least 20% more successful records/s or 20% lower client CPU per successful record than the best relevant baseline, at equal durability/security and latency budget. | Multi-run x86_64/arm64 evidence; p99 no worse by more than 5% at matched load, all losing cells visible. |
| Ergonomics and operations | Two newcomers complete a producer/consumer guide in 15 minutes after prerequisites; enabled telemetry overhead initially budgeted at 2%. | External-consumer examples, feedback, profiles, redaction/cardinality tests and a diagnosis/rollback exercise. |

## 4. Ordered implementation packages

Each package may require several small PRs. Existing commands below are
building blocks, **not proof that new acceptance criteria already pass**.
New harnesses must be added and wired into CI before their package can close.

### KL-01: Recover the baseline and establish protocol oracles

**Priority:** P0. **Depends on:** none.

1. Reconcile frozen notes with HEAD and name an owner for every unfinished
   item. Confirm the incoming formatting/CI-policy repairs, reproduce relevant
   latency configurations on controlled hardware, and distinguish harness noise
   from a client regression. Preserve integrity checks and historical failures.
   **Partial (2026-09-05):** shared-runner vs local-native vs controlled-host
   budgets recorded in [latency-ci-policy.json](latency-ci-policy.json);
   nested 1,344/750 µs miss is historical (`SKIP_LATENCY_GATE` on integrity).
   GHA 5000 µs and local 750 µs relative gate unchanged. Controlled-host
   reproduce remains open. Not Done.
2. Reconcile the requested versus actual broker identity and require explicit
   capability/result records. Address developer-host prerequisites: the broker
   script invokes GNU `timeout`; declare or provide a tested supported path
   rather than assuming it exists on macOS.
   **Partial (2026-09-05):** `scripts/lib/broker-identity.sh` stamps `actual=` on
   broker/auth smoke; `scripts/lib/pl-timeout.sh` prefers `timeout` then
   `gtimeout` (Homebrew coreutils) and fails closed with an install hint.
3. Compare Produce, Fetch, Metadata and ListOffsets first against pinned
   Kafka Java clients/brokers 3.9.1 and 4.1.0; expand by API coverage and user
   demand. Compare decoded semantics and required fields, not arbitrary byte
   ordering, client IDs or correlation IDs.
   **Partial (2026-09-05):** `tests/protocol_oracles.rs` plus
   `scripts/ci-protocol-oracles.sh` compare decoded required fields for
   Produce/Fetch/Metadata/ListOffsets against fixture pins 3.9.1 and 4.1.0;
   live broker optional via `REQUIRE_BROKER=1`. Does not close KL-01.
4. Extend existing [fuzz targets](../fuzz) beyond short CI smoke, focusing on
   lengths, tagged fields, truncated batches, CRC, allocation and decompression
   bounds. Retain minimized failures and campaign/coverage metadata.
   **Partial (2026-09-05):** campaign metadata harness (`scripts/ci-fuzz-campaign.sh`,
   `fuzz/campaign/metadata.example.json`) is distinct from 15s CI smoke; minimized
   failures are retained under `fuzz/artifacts/minimized/`. Not a sustained-campaign close.

**Work surfaces:** [protocol](../src/protocol), [fuzz smoke](../tests/fuzz_decode_smoke.rs),
[broker smoke](../scripts/ci-broker-smoke.sh), [integrity smoke](../scripts/ci-integrity-smoke.sh),
[latency gate](../scripts/ci-latency-gate.sh).
**Done when:** required CI is green without silent skips or weaker assertions;
each advertised matrix cell names the actual broker and cases; differential
fixtures and sustained campaigns have linked results and no unresolved failures.

### KL-02: Bound memory and define cancellation/shutdown outcomes

**Priority:** P0. **Depends on:** KL-01's supported contract.

1. Trace ownership from caller enqueue through worker/retry queues, batch
   encoding, sockets and fetch buffers. Account for queued **and in-flight**
   bytes, compressed/uncompressed storage, connections and spawned tasks.
2. Verify current queue/time-limit behavior before changing defaults. Define
   cancellation before enqueue, while buffered, after send, and after broker
   acknowledgment. A dropped future must not imply a record was never written.
   **Partial (2026-09-05):** guide cancellation table + `Producer::send`/`close` rustdoc;
   mock tests in `tests/produce_cancel.rs`; `close` sets a durable `closed` flag so clones
   cannot respawn workers. Remaining: overload soak, full byte/task ownership trace.
   **Partial (2026-09-05):** consumer `leave`/`close`/`unsubscribe` no longer
   auto-commit positions (`tests/consumer_close_commit.rs`); poll-interval
   auto-commit unchanged.
3. Exercise saturating senders, slow consumers, stalled brokers, shutdown and
   timeout races. Specify whether each operation drains, rejects or reports an
   unknown outcome. Never auto-commit unprocessed consumer offsets on close.

**Work surfaces:** [producer](../src/producer.rs), [consumer](../src/consumer.rs),
[config](../src/config.rs), [cluster](../src/cluster.rs), [network](../src/net.rs).
**Done when:** 2x-overload and cancellation histories obey the configured
resource model; no hung completions or leaked permits/tasks remain after drain;
all accepted work has a documented completed, failed or ambiguous outcome.

### KL-03: Prove HA, transactions and group semantics with crash histories

**Priority:** P0 for the corresponding production profile. **Depends on:** KL-01/02.

1. Extend smoke to a three-broker KRaft topology with controller quorum,
   replication factor 3 and explicit `min.insync.replicas`. Exercise leader and
   coordinator moves, lost responses, socket partitions, process pause/restart,
   rolling upgrades, stale metadata and topic/partition changes.
2. Record unique record IDs, payload hashes and per-key order before submission,
   after acknowledgments and in independent consumers. Classify failed and
   unknown outcomes; compare full histories, not just high-watermark deltas.
   Broker offsets include control records and are not a count of application
   records in transactional tests.
3. Test PID/sequence/epoch recovery, fencing, transaction commit/abort, coordinator
   failover and `read_committed` output plus committed input offsets against
   version-pinned Java behavior. Follow broker/protocol recovery rules; never
   invent a local producer-epoch increment as a generic retry strategy.
4. Cover classic/cooperative/KIP-848 membership churn separately from share
   acquisition, lock expiry, release/reject and redelivery. Define which forms
   of duplicate delivery are expected for each API. Verify assignments, progress,
   offset safety and coordinator reconnection under TLS/SASL.

**Work surfaces:** [producer](../src/producer.rs), [consumer](../src/consumer.rs),
[group](../src/group.rs), [share](../src/share.rs), [transaction protocol](../src/protocol/txn.rs),
[live tests](../tests/kip848_live.rs), [broker smoke](../scripts/ci-broker-smoke.sh).
**Done when:** seeded histories satisfy each profile's invariants and recovery
budget across supported broker versions, with no unexplained outcomes.
`acks=all` alone is not an exactly-once guarantee.

### KL-04: Establish reproducible leadership, then optimize measured limits

**Priority:** baseline early; optimization after correctness. **Depends on:** KL-01;
KL-02/03 before production-performance claims.

1. Freeze separate low-latency, bulk-throughput, fetch, transactional and
   mixed-load workloads. Compare pinned rust-rdkafka/librdkafka and Java, plus
   another Rust client when its semantics match. Preserve the existing Lab A
   contracts and compare separate producer, consumer and end-to-end results.
   **Partial (2026-09-05):** CI vs local vs controlled-host budgets recorded
   in [latency-ci-policy.json](latency-ci-policy.json). Shared-runner smoke
   is not Lab A; unsigned samples must not lift Suite HOLD. Controlled-host
   qualification remains open. Not Done.
2. Match acks, idempotence, isolation, replication/ISR, compression, batch/linger,
   partition count/skew, keys, connections, in-flight bounds, TLS/SASL and payload
   distribution. Distinguish enqueue acceptance from broker acknowledgment and
   independent consumption; acknowledgment is not proof of a per-record fsync.
3. Extend [bench_latency](../examples/bench_latency.rs) with scheduled open-loop
   arrivals through saturation. Include intended-send delay and rejected/timed-out
   work to avoid coordinated omission; do not report only successful fast samples.
4. Collect p50/p95/p99/p99.9, sample counts, successful records/s, client/broker
   CPU, allocations, RSS and lag. Separate hosts/processes, test real-network
   RTT, randomize A/B order, and retain at least five long-enough repetitions per
   cell with uncertainty intervals on x86_64 and arm64.
5. Profile serialization, compression, batching, allocation, syscalls, routing
   and executor contention. Land one explained, reversible optimization per PR.
   Reject apparent wins from weaker durability, hidden errors or one bespoke schema.

**Work surfaces:** [benchmark examples](../examples), [latency gate](../scripts/ci-latency-gate.sh),
[producer](../src/producer.rs), [network](../src/net.rs), [record codec](../src/protocol/records.rs).
**Done when:** an independent operator reproduces named scorecard wins from
raw artifacts and exact configs/revisions. Publish losses too. Performance
signoff follows [benchmark.md](benchmark.md)/Suite HOLD; no new plan bypasses it.

### KL-05: Expand codec and ecosystem coverage only when justified

**Priority:** P1/P2, demand-gated. **Depends on:** KL-01; KL-04 for performance claims.

1. Evaluate zstd decompression and compression separately for Kafka framing,
   error handling, memory, throughput, maintenance and licensing. `ruzstd` is
   a decompression candidate, not an assumed complete encode/decode solution.
2. Require independent encoder/decoder interoperability, malformed-input tests
   and bounded decompression. Select a backend only after a recorded decision.
   Current [deny policy](../deny.toml) bans `zstd-sys`; even optional native
   support needs an explicit policy/design decision, not a quiet feature flag.
3. Keep Schema Registry in the [companion design](schema-companion.md).
   Evaluate GSSAPI, broader admin APIs and other gaps by named adopter need,
   lifecycle cost and alternatives, rather than treating them all as GA blockers.

**Work surfaces:** [records](../src/protocol/records.rs), [Cargo.toml](../Cargo.toml),
[zstd spike](zstd-spike.md), [gaps](gaps.md).
**Done when:** selected scope has interop/safety/performance evidence and an
honest dependency footprint. Deferred features remain documented exclusions.

### KL-06: Qualify authentication and transport recovery

**Priority:** P0 for supported secure deployments; extensions demand-gated.
**Depends on:** KL-01/02.

1. Audit existing SASL/OIDC refresh and connection behavior before adding new
   APIs. Test expiration, clock skew, token endpoint outages, reconnect,
   broker-requested reauthentication, TLS root/certificate rotation and mTLS.
2. Define bounded refresh/backoff and credential lifetime handling. Fail closed
   when authentication or TLS validation fails, without plaintext fallback.
   Distinguish a refresh-fetch failure while credentials remain valid from
   invalid credentials; do not force unnecessary reconnect storms.
3. Exercise recovery with producer, consumer, admin and group-coordinator paths.
   Verify redaction in Debug, errors, spans and metrics, including failure paths.
   **Partial (2026-09-05):** `Debug` redacts Sasl passwords, OIDC `client_secret`,
   and mTLS key PEMs (plus producer/consumer/admin config cascade); see
   [security.md](security.md) and `tests/credential_redact.rs`. OIDC/OAUTHBEARER `Error` bodies no longer embed IdP/broker payloads (2026-09-05).
   Spans/metrics redaction and rotation/outage recovery remain open.

**Work surfaces:** [network](../src/net.rs), [OAuth](../src/protocol/oauth.rs),
[OIDC](../src/protocol/oidc.rs), [auth smoke](../scripts/ci-auth-smoke.sh),
[security policy](security.md).
**Done when:** short-lived-credential fixtures and real secure broker cases
cover multiple rotations/outages with documented outcomes, bounded recovery
and no secret exposure. Preserve existing audit/deny and security-reporting lanes.

### KL-07: Make adoption and diagnosis simpler than the alternatives

**Priority:** P0 for basic usability. **Depends on:** KL-01; KL-02/03 for recipes.

1. Compile the existing producer/consumer/transaction and
   [rust-rdkafka migration](migrate-from-rdkafka.md) examples as external package
   consumers. Explain enqueue versus acknowledgment, ownership, partitioning,
   offset commit, cancellation and exactly-once boundaries without Java API guesses.
2. Extend existing metrics and optional tracing only where diagnosis is missing:
   queue age/bytes, retries, broker throttle, reconnect, ack latency, lag,
   rebalance and transaction outcomes. Bound label cardinality; provide optional
   exporters as examples rather than mandatory core dependencies.
3. Have two independent users follow the guide and diagnose a throttled broker,
   stale leader and blocked consumer using telemetry rather than payload logging.

**Work surfaces:** [metrics](../src/metrics.rs), [interceptors](../src/interceptor.rs),
[guide](guide.md), [examples](../examples), [consumer fixture](../scripts/ci-crate-consumer.sh).
**Done when:** examples run from a fresh package consumer, usability feedback
is resolved, and disabled/enabled instrumentation costs and redaction are measured.

### KL-08: Gate releases and promote through reversible adoption

**Priority:** P0 release safety; promotion after applicable packages.
**Depends on:** none for stopping release/CI divergence; applicable profile gates
for adoption and API stabilization. Optional KL-05 features do not gate everyone.

1. Select one serialized publication path across
   [release-plz](../.github/workflows/release-plz.yml) and legacy workflows.
   Require green evidence for the exact release SHA, isolated package consumers,
   author/license/notice inventory and an idempotent partial-release recovery.
   Reconcile [release policy](RELEASE.md), metadata checks and stale handoff
   scripts with actual state. Scope "no C" metadata to the Kafka implementation,
   not the entire TLS dependency graph. Rehearse without publishing.
   **Partial (2026-09-05):** release-plz is PR-only (never auto-publish). Canonical publish is
   tag → `release.yml` / `owner-cut-release` with exact-SHA `check-main-ci` + `ci-crate-consumer`.
   Rehearse with `DRY_RUN=1`; do not cut a new version from this slice.
   **Partial (2026-09-05):** `scripts/rehearse-partial-release.sh --self-test` proves idempotent
   recovery without publishing (0.1.0 stays; day1/handoff DRY_RUN, not another publish).
   `owner-publish` skips `cargo publish` when the version is already on crates.io;
   `release.yml` `actions: read` unblocks exact-SHA `gh run list`. KL-08 stays open.
2. Specify broker/API, OS/architecture, MSRV and feature support plus security
   response, upgrade and deprecation policies. Keep unsupported combinations
   explicit; existing Rust 1.85/stable CI is a starting point, not a new promise.
   **Partial (2026-09-05):** [`support.md`](support.md) records the CI-backed
   matrix (Kafka 3.9.1/4.1.0, MSRV 1.85, Linux/x86_64, default pure-Rust features)
   and explicit non-promises. Linked from RELEASE/ADOPTION/api-stability. Does
   **not** close KL-08 (adopter 24h/7d records and promotion/rollback remain).
3. Qualify two independent adopter workloads with 24-hour then 7-day runs.
   With operator approval, shadow into isolated topics/read-only consumers
   before a 1%/10%/50% traffic rollout. Never duplicate real downstream side
   effects or assume a consumer group can arbitrarily assign a traffic percentage.
4. Compare IDs/payloads, not offsets across distinct shadow topics. Rehearse
   draining, transaction fencing/abort and resuming from correct committed
   offsets before rollback; do not auto-commit unprocessed records.
5. Stop immediately on integrity, credential or resource-bound violations.
   Initial rollback targets: unexpected-error rate > baseline by 0.1 percentage
   points or p99 > baseline by 10% for two 5-minute windows; freeze sample floors
   and any stricter service SLO beforehand. Keep rollback operator-controlled
   unless separately approved automation has been rehearsed.

**Done when:** maintainers/operators sign off on linked per-profile evidence,
no blocker remains for that profile, and upgrade/rollback is demonstrated.
A separate 1.0 decision includes API and maintenance commitments; no date or
version bump is implied by finishing a feature checklist.

## 5. First PRs and proof discipline

| Order | Small first delivery | Then |
|---|---|---|
| 1 | KL-01: reconcile frozen notes and incoming recovery fixes, confirm current required lanes and make broker identity/prerequisites explicit. | Preserve every unresolved failure as a named item; do not redo landed fixes. |
| 2 | KL-01/KL-04: reproduce the latency failure, separate shared-runner smoke from controlled performance qualification and record justified budgets. | Do not mark the failure fixed just by increasing a threshold. |
| 3 | KL-08: make release eligibility depend on exact-SHA required CI and package-consumer evidence. | Then expand KL-01/02/03 correctness and KL-04 measurements in parallel. |

Existing proof entry points (from the repository root):

| Purpose | Command | Limit |
|---|---|---|
| Local fast lane | `bash scripts/ci-branch-lite.sh` | Not the complete CI matrix. |
| Unit/mock and optional feature behavior | `cargo test --all-targets`; `cargo test --all-targets --features tracing` | Ignored live suites do not run automatically. |
| Decode smoke | `cargo test --test fuzz_decode_smoke` | Not a sustained fuzz campaign. |
| Broker and auth smoke | `REQUIRE_BROKER=1 bash scripts/ci-broker-smoke.sh`; `REQUIRE_AUTH=1 bash scripts/ci-auth-smoke.sh` | Starts local broker infrastructure; inspect prerequisites and actual broker versions. Not HA qualification. |
| Count and latency smoke | `bash scripts/ci-integrity-smoke.sh` | Count equality is not an exactly-once oracle. The historical nested latency failure is now separated from the integrity CI job. |
| Rust documentation | `bash scripts/ci-docs.sh` | Builds rustdoc and checks intra-doc warnings, not Markdown file links. |

Close a TODO only with a PR, exact source/tool/peer revisions, configuration,
commands, raw artifacts, observed result, exclusions and reviewer signoff.
Record owners and next actions for failures. Keep evidence dated and scoped;
neither a feature inventory nor a crates.io upload can substitute for it.
