# Quota errors and Kafka response boundaries

`quotas::Window` is an in-memory admission utility. It does not enforce
listener quotas, send Kafka throttle responses, or establish fairness among
connections. This change corrects a local error mapping; it does not add those
features.

`quotas::Error::code()` previously returned `Some(89)` for every
`Throttled` refusal. It now returns `None`, because the local error does
not identify a Kafka API, version, or the cause of the refusal. This is a
behavior-breaking API change even though its Rust signature is unchanged.
The `THROTTLING_QUOTA_EXCEEDED` constant remains 89 for APIs and versions
that define it. The broker manifest has `publish = false` and version 0.1.0,
but downstream Git users may rely on the old result. On 2026-11-08 the
[crates.io Git index](https://github.com/rust-lang/crates.io-index) had no
`pa/rt/partitionline-broker` entry; the `partitionline` client entry was
present. The broker README and unreleased changelog describe migration.

Deprecating `code()` while retaining `Some(89)` would leave the misleading
mapping active, and can introduce warning failures for downstream users.
An API/version-specific mapping needs the operation and policy in addition to
the refusal; the current `Throttled` variant combines rate, connection,
identity-state, in-flight, and internal-capacity failures. Searching 45
non-excluded top-level broker source, test, and documentation files, plus the
operator entrypoints and clock integration test, found no use of this
mapping. That limited search does not establish that external or unsearched
callers do not exist.

## Protocol evidence

Apache Kafka 4.1.0 source at commit
[`13f70256db3c994c590e5d262a7cc50b9e973204`](https://github.com/apache/kafka/commit/13f70256db3c994c590e5d262a7cc50b9e973204)
handles Produce bandwidth quota by muting the channel and including a throttle
time in a normal response; for `acks=0` it sends no response but still applies
bandwidth muting. See [KafkaApis.scala](https://github.com/apache/kafka/blob/13f70256db3c994c590e5d262a7cc50b9e973204/core/src/main/scala/kafka/server/KafkaApis.scala#L479-L520).
[ProduceResponse.java](https://github.com/apache/kafka/blob/13f70256db3c994c590e5d262a7cc50b9e973204/clients/src/main/java/org/apache/kafka/common/requests/ProduceResponse.java#L294)
enables client-side throttling from Produce version 6. In contrast,
[CreateTopicsResponse.json](https://github.com/apache/kafka/blob/13f70256db3c994c590e5d262a7cc50b9e973204/clients/src/main/resources/common/message/CreateTopicsResponse.json#L24-L38)
defines error 89 for controller mutation quota starting at version 6. These
are pinned source observations, not a live interoperability test or a claim
that only these operations may use error 89.

## Changes and checks

The [candidate](https://github.com/mingley/partitionline/commit/55395ae7743428b73cf7903199e4e242af9a307a)
is based on `a0ad79df8a5d9872eacd1cd322b59d8d979f04e3`.
Its files are pinned by Git blob:

| File | Blob |
| --- | --- |
| `partitionline-broker/src/quotas.rs` | `fe6ed7dabd863ec9e1b3f54101d57ea4d2abc6a0` |
| `partitionline-broker/tests/quotas_error_code.rs` | `5bd046aee1fcab6d0bf1d2bf6eddeecd2f31a4c4` |
| `partitionline-broker/README.md` | `ad8cdb5e9d73f18f2395acf11bcbd4e0f66436ff` |
| `CHANGELOG.md` | `8aa813e7331b4c96095b02e079b0b39bc1ce92c7` |

The public controls exercise global and per-client connection limits,
identity-state capacity, in-flight limit and overflow, and produce, fetch,
and replication rate refusals. They also check the unchanged constant and
actual `InvalidRequest`, `UnknownWork`, and `InvalidLimits` returns. Two
unit controls construct work-ID exhaustion and debt overflow with private
state, since reaching these cases through public calls is not practical.

The corrected x86-64 [qualification 42494010266](https://github.com/mingley/partitionline/actions/runs/42494010266)
used stable Rust 1.99.1 and LLVM 23.1.1. In both default and all-feature
profiles, the baseline public suite reported two passes and eight expected
failures, and both private exhaustion controls failed as expected because
the old mapping returned `Some(89)`. The candidate passed 10/10 public
controls, 13/13 existing clock controls, and 12/12 quota unit tests per
profile.

The baseline source blob is `75bca5a99c2dd14d3b28c1bb3fe924bfebb3cf0e`;
it contains the same private tests and the original mapping. Baseline and
candidate used identical integration-test and lockfile bytes, separate Cargo
target directories, and different test-executable SHA-256 hashes. The
candidate source and public test SHA-256 values are
`f61f02a22b6f11a03d285b3921a6c115815bd88d8ef1c51b2ed3edda8bf0ee9c` and
`3d18932fb01ff86dedfcb6cd6a3d5591043b77591fec1c0bf402e537e9ba1c05`.

The earlier five-control qualification
[42492037662](https://github.com/mingley/partitionline/actions/runs/42492037662)
passed both feature profiles and strict checks on the earlier candidate; it
did not cover the added cases. In the expanded run
[42492581946](https://github.com/mingley/partitionline/actions/runs/42492581946),
the baseline public controls showed the expected 2 passes and 8 failures in
both profiles. Both behavior jobs then failed due to a test selector typo:
`has_no_generic_kafka_error` selected the debt-overflow unit test but omitted
the work-ID unit test, whose name uses `have`. The selected debt test failed
as expected, but the harness required both, so candidate behavior and
separate-binary checks did not run. Stable formatting, strict all-target
all-feature Clippy, and all-feature Rustdoc with warnings denied passed on
the final candidate in that run. The failed run and its logs remain available.

The standard locked all-target broker suites ran in
[42494566513](https://github.com/mingley/partitionline/actions/runs/42494566513)
on the exact candidate above, with default and all features on x86-64. Both
profiles failed and stopped before later test executables ran:

- Default: 727 passed, three failed, four ignored across 27 executables.
  The `raft_runtime` executable reported nine passes and three failures.
- All features: 666 passed, one failed, four ignored across 16 executables.
  The `operator_quorum` executable reported one pass and one failure.

No targeted retry or investigation was performed, and these results do not
establish the cause of the failures. The logs are retained with the run. The
candidate is based on `a0ad79...`; current main at `0883c3...` also has a
later interoperability-peer change and documentation updates. This full
suite does not test the combined current-main tree. At that main revision,
the pre-change quota source, broker README, and root changelog blobs still
match the candidate base, and the new public test file is absent.

These checks do not measure speed or establish production readiness.

## Wire work still needed

The [request header](https://github.com/mingley/partitionline/blob/55395ae7743428b73cf7903199e4e242af9a307a/partitionline-broker/src/protocol.rs#L100) permits a nullable client ID
while [`Window`](https://github.com/mingley/partitionline/blob/55395ae7743428b73cf7903199e4e242af9a307a/partitionline-broker/src/quotas.rs#L440) requires a nonempty, bounded
identity. [`Peer`](https://github.com/mingley/partitionline/blob/55395ae7743428b73cf7903199e4e242af9a307a/partitionline-broker/src/transport.rs#L46) exposes an unauthenticated
address and an optional verified SASL identity;
[`Router::exchange_request`](https://github.com/mingley/partitionline/blob/55395ae7743428b73cf7903199e4e242af9a307a/partitionline-broker/src/metadata.rs#L1010) currently ignores
peer context. Quota policy needs an explicit authenticated-principal
and client-ID mapping, including anonymous and changing client IDs, plus a
trusted monotonic clock. The window trusts future caller timestamps and
uses token buckets; the Kafka reference uses a different quota accounting
implementation.

The data actor retains an [admission lease](https://github.com/mingley/partitionline/blob/55395ae7743428b73cf7903199e4e242af9a307a/partitionline-broker/src/metadata.rs#L262) across
queued work and undelivered completion. Quota ownership must likewise survive handler cancellation,
actor execution, reply handoff, disconnect, and shutdown; releasing only when
a handler future drops would miscount work. The [Produce writer](https://github.com/mingley/partitionline/blob/55395ae7743428b73cf7903199e4e242af9a307a/partitionline-broker/src/produce.rs#L2125) currently
writes a zero throttle time, and transport needs a bounded channel-muting
mechanism, including the no-response `acks=0` path. The default handler
timeout is 30 seconds while the local window permits delays up to 60
seconds, so placing a sleep inside a handler also changes timeout behavior.

A complete implementation needs API/version-specific response handling,
clear byte accounting and cancellation ownership, a monotonic time source,
bounded identity and timer state, and integration tests against real Kafka
clients and a reference broker. Idle identity reclamation and timed rate
updates can scan client state synchronously; bounded identity count alone is
not a bound on latency, total process memory, or fairness.
