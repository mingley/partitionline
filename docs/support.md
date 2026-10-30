# Support matrix

This describes CI coverage for the current source. A configured lane can fail;
see [current validation](STATUS.md#validation) for recorded results.
Published 0.1.0 is a separate source baseline.

API changes follow the [0.x policy](api-stability.md). Cuts are described in
[RELEASE.md](RELEASE.md). Adopter steps are in [ADOPTION.md](ADOPTION.md).
Broader correctness, fault, and performance work stays in the
[task queue](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/docs/plan/README.md).

The [2026-09-21 source audit](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/docs/audits/2026-09-21.md) reproduced five consumer
correctness cases at `cb7e97d`. Those cases are fixed in later source.
Apache 4.1.2, 4.2.1, and 4.3.1 have their own digest-pinned profiles. The
older 3.9.1 and 4.1.0 smoke runs are historical coverage.

## CI coverage

| Dimension | Configured scope | Evidence |
|---|---|---|
| Crate version | `0.1.0` on crates.io | Installable; do not re-cut `0.1.0` |
| Rust | Latest stable; currently **1.99** (`rust-version` in `Cargo.toml`) | Stable-only CI for the client, broker and schema companion. Older compilers are not supported. |
| Host OS (CI) | Linux (`ubuntu-latest`); native macOS (`macos-15`) and Windows (`windows-2025`) for mock/runtime and packed consumers | `.github/workflows/ci.yml`; KL08-05 / KL08-14 native default/tracing cells |
| Host arch (CI) | Linux `x86_64`; macOS `arm64`; Windows `x86_64` MSVC | Native compiler/runtime guards and retained platform reports |
| Brokers (current source) | Apache Kafka **4.1.2**, **4.2.1**, **4.3.1**; historical smoke on **3.9.1** / **4.1.0** | `broker-current` required profiles and separate `broker-smoke` history; [frozen image digests](../tests/conformance/current-broker-cells.json) |
| Default features | gzip through `zlib-rs`; TLS through rustls and Ring | `Cargo.toml` defaults + deny/audit lanes |
| Auth in smoke | SASL PLAIN / SCRAM / OAUTHBEARER + rustls TLS (when auth smoke runs) | `scripts/ci-auth-smoke.sh` (soft-skip without Java/Kafka unless `REQUIRE_AUTH=1`) |

## What a current broker profile checks

KL01-10 runs the three current broker distributions at source `8bec4f2` in
[CI run 36869103809](https://github.com/mingley/partitionline/actions/runs/36869103809).
Each cell is one fresh Linux x86_64 broker, replication factor 1. It checks
admin calls, 16 produced records, a manual consumer, one-member classic,
cooperative, and KIP-848 groups, share accept and close, and a transaction
that commits output and offsets with a hidden abort sentinel. Rust checks
identities, offsets, timestamps, and payloads. The pinned Java CLI reads both
histories and checks four groups' committed offsets and zero lag. A missing
API or a disabled required feature fails the profile. The commit marker is
observed within five seconds, without running the transaction again.

KL01-17 runs source `9404187`. The retained evidence is
[KL01-17.json](https://github.com/mingley/partitionline/blob/80b4c78a4399ed5cfc68c7a3cb14046f008e9d78/docs/plan/evidence/KL01-17.json).
The Apache 4.1.2 lane also runs the
[frozen live scenario](../tests/conformance/verifiable-live-profile.json):
both Rust adapter binaries, one fresh partition, 25 null-key decimal values,
and a Java CLI check of every record plus committed offset 25 and lag 0.
Each attempt keeps its source SHA, compiler, image, streams, and failures.
This is one producer and consumer scenario. It is not the full ducktape suite
or the full SDK case registry. The earlier mock-only case is historical.

These profiles use current source and default features. They do not cover:

- Multi-broker HA, rebalance churn, or share lock expiry
- Crash and fencing
- External auth
- A live broker on macOS or Windows
- Tiered storage
- Performance

Historical smoke does not excuse a failed current profile. Published `0.1.0`
is a separate source baseline.

## Platform lanes

KL08-05 keeps native macOS arm64 results on `macos-15`: default and tracing
runtimes, TLS mock paths, and packed-crate consumers. The recorded hosted runs
passed. Current CI uses latest stable Rust only. Broker and performance
qualification stay on Linux.

macOS needs Xcode command-line tools, Python 3.11 or newer, and Homebrew Bash
5 or newer and OpenSSL 3 on `PATH`. Apple Bash 3.2 cannot run the package and
documentation scripts' empty arrays under `set -u`. OpenSSL only mints
ephemeral mock certificates. It is a test executable, not a crate dependency.
The lane records the OS, architecture, compiler, OpenSSL, Bash, and Python
versions. Those runs apply to the sources they recorded.

KL08-14 keeps native `windows-2025` x86_64 MSVC results: default and tracing
runtimes, public TLS mock paths, and packed consumers. The recorded hosted
runs passed at source `5b02bc5`. Current CI uses latest stable Rust only.
Windows live-broker, external auth, and performance runs are not qualified.

Windows needs MSVC C build tools for Ring, native 64-bit Python 3.11 or newer
in UTF-8 mode (`PYTHONUTF8=1`), Git Bash 5 or newer, and OpenSSL 3 on `PATH`.
The driver maps the scripts' `python3` calls to setup-python's `python.exe`
and records that certificate tool. A cross-build, WSL, a skipped runtime, or
a compile-only result does not qualify this cell.

## Not promised

Each row names its KL05-01
[feature-registry](../tests/conformance/features.json) entry where one exists.
The registry also records independent qualification gaps. Platform and
production qualification stay separate.

| Item | Status | Registry |
|---|---|---|
| Kerberos / GSSAPI | Not in default features; no CI promise | `auth.sasl_gssapi` (`missing`) |
| zstd | Optional bounded encoding/decoding with `zstd-rs`; default dependencies unchanged | `codecs.zstd.decode/encode/wire_helper` (`present`, finite qualification) |
| Schema Registry as part of this crate | Outside core; unpublished companion has bounded read-only lookups/cache and Protobuf indexes with a caller-selected codec. Applications select Avro/JSON serializers explicitly. | `schema_ecosystem.registry_client/cache/protobuf` (`present`, companion scope); `avro/json_schema` and generic `wire_framing` (`present`, bounded offline qualification) |
| Multi-broker chaos / HA proof | KL-03 still open | `manual_consumer.fetch` (`partial`); heartbeat/throttle scheduling `partial` |
| OIDC token lifecycle | Application-owned `OidcTokenManager` supports refresh; shared connection-open caching has bounded idle retention. Broader connection renewal qualification remains separate. | `auth.sasl_oidc_refresh`; see [security](security.md#token-acquisition-and-lifecycle) |
| Sticky unkeyed partitioner | Current source has opt-in `StickyPartitioner`; round-robin remains the default. Independent qualification is in progress (KL05-10). | `producer.sticky_partitioner` is a stale `missing` registry entry pending reconciliation. |
| Signed Suite HOLD / Lab A | **Unsigned** — Suite HOLD remains | — (qualification gate, not a feature entry) |
| macOS Intel / other host architectures | May build; no CI support promise for these cells | — (CI dimension, not a feature entry) |
| Every Kafka API version | Demand-led (KL-05); see [gaps.md](gaps.md) | `producer.v13_wire` (`present`); `manual_consumer.v18_wire` (`present`) |

## What recent source adds

These notes describe the working source. They do not by themselves qualify a live multi-broker deployment.

### Wire behavior added in current source

| Area | What the source does | What is not qualified |
|---|---|---|
| Produce v13 | Topic ids. Checked against Apache 4.1.2, 4.2.1, and 4.3.1 serialization, live Java decode, topic recreation, a frozen identity snapshot, missing ids, delivery-budget retries, and the v3–v12 fallback. Transaction version 1 stays at Produce v11. Finalized `transaction.version` of 2 or higher, with EndTxn v5, enables v12 and v13. That choice is fixed when the producer starts. | A live multi-broker deployment |
| ListOffsets v1–v11 | Includes earliest pending upload, `-6`. An unsupported selector or isolation level is rejected before the request. The deadline covers metadata, connect, negotiation, the RPC, and retries. | A live tiered-storage deployment and the pending-upload boundary |
| ListTransactions v0–v2 | A complete listing and a per-broker listing. A nonempty id pattern requires v2 and is not dropped on an older broker. | Live transaction-state recovery |
| Metadata v0 and GROUP FindCoordinator v0 | Spoken. A named v0 metadata request requires automatic topic creation. Missing controller, UUID, and rack fields keep their sentinels. The current Java Metadata builder refuses v0. | |
| OffsetCommit and OffsetFetch v10 | Topic UUIDs on typed requests, on `ConsumerGroup`, and on Admin. Name-based calls keep the older formats. A UUID-only request refuses an older coordinator. A recreated topic name does not receive the old commit. | Live coordinator persistence and cluster failover |
| DescribeLogDirs v1–v5 | `IsCordoned` appears at v5. Older versions default it to false. One ApiVersions RPC is inside that broker hop's deadline. A struct literal needs `is_cordoned`. The constructor defaults it to false. | Live v5 broker behavior |
| Streams v0 | Caller-driven heartbeats and typed Admin descriptions, including caller order and duplicates. Failures are reported per group. This client has no Streams engine, and the broker has no Streams coordinator. | A Streams runtime |

### Quorum admin

`Admin` has `elect_leaders`, read-only `describe_quorum`, `add_raft_voter`,
and `remove_raft_voter`.

| Piece | Behavior |
|---|---|
| RemoveRaftVoter | Version 0 only |
| AddRaftVoter | Negotiates v0 or v1 |
| Default acknowledgement | Waits for a committed voter set. v0 has no other mode. |
| `ack_when_committed: false` | A local append. Requires v1. A v0-only broker is rejected before I/O. Success is not committed membership. |
| Java's public options | Expose cluster id only. The false mode is a Rust extension. |
| Routing | Broker forwarding. One deadline covers voter, directory, and cluster checks. Addition also checks named endpoints. Removal has no TimeoutMs field. |
| What is checked | Independent Apache v0 fixtures and mock v1 responses |
| What is not checked | A live delayed commit, a real membership change, or a direct controller bootstrap |

A lost response can leave membership ambiguous. Inspect the quorum before
another change. A production membership change needs an operator-approved
procedure.

### Published crate

crates.io `partitionline` 0.1.0 is tag `v0.1.0` (4 September 2026, 8:28 PM CT). The [migration guide](migrate-from-rdkafka.md) is the current map. It names files and symbols.

An older check compared commit `ca50ca1` (22 September 2026, 1:08 PM CT) with that tag. At that commit, 14 files under `src/` differed from the published crate. That count belongs to the check. Current source is later than both the tag and `ca50ca1`.

The published crate predates the buffer limits, record representations and
compiler requirement in the [unreleased changes](../CHANGELOG.md#unreleased).
The next client release must be at least 0.2.0. Build configurations with
builders so new fields do not require exhaustive struct literals.

The 0.x caller contract for error categories (retryable, abort-required, fatal, unsupported, timeout, ambiguous delivery) and for public config fields is in [api-stability.md](api-stability.md). Match error categories, not `Display` text. Build configs with the builders, or with `Default` plus overrides.

Report vulnerabilities privately. See [security.md](security.md). Credential `Debug` redaction is documented there. It does not replace the live credential-lifecycle matrix (KL06-06), which is still pending.

### Upgrades

Prefer a patch release for fixes. A breaking change to a Stable surface bumps `0.MINOR`. See [RELEASE.md](RELEASE.md) and [api-stability.md](api-stability.md). Combinations listed above as unsupported may change while the crate is on 0.x. They will not be advertised as supported in this matrix. Rolling back a production cut is the operator’s job. Release rehearsal covers partial-release recovery without publishing (`scripts/rehearse-partial-release.sh`).

### Still open for KL-08

This matrix does not close KL-08. Still open: two independent adopter 24-hour and 7-day records, traffic-shadow promotion, and an operator-approved rollback proof under production SLOs. The [adopter exercise template](adopter-exercise.md) is blank. Filling it in is how a run becomes evidence. The template itself is not evidence.

### Quotas and incremental fetch

No live quota deployment is claimed. Admin, heartbeat, and group quotas
are separate from Produce and Fetch.

| Path | Versions the client delays | Versions left to the broker | What a positive wait uses |
|---|---|---|---|
| Produce | v6–v13, every connection to that broker | v3–v5 | The existing delivery deadline and close budget |
| Fetch | v8–v18, per broker, timed from response completion | v4–v7 | The poll long-poll and the original request budget. Wakeup and caller cancellation interrupt it. |

`ProducerMetrics.throttle` and `ConsumerMetrics.throttle` are fixed-size
snapshots of requested time. A negative Produce throttle is invalid and is
counted, not applied. While one Fetch peer is muted, other brokers and
buffered records keep delivering. Peer tests cover isolation, in-flight
acknowledgements, reconnection, version boundaries, zero and negative
durations, a discarded response at the buffer cap, expiry, cancellation,
and shutdown. The Apache Java 4.3.1 Fetch scheduling reference is executed
separately. No throughput improvement is claimed.

| Fetch behavior | What is true |
|---|---|
| Sessions | One broker-local session on v7–v18. v4–v6 sends a full fetch. |
| Checks | Partition and identity checks, bounded session-error recovery, assignment and topic-id changes, and a terminal close |
| 32-partition runner | One named reconnect reset and 65 exact Rust and Java records |
| Apache 4.3.1 handler | 80 state transitions, executed separately |
| Request size | Reduction is measured. Throughput, latency, and a sustained broker-fault result are not. |
| Fetch v18 | Replica high-watermark sidecars with strict lengths for known tags. Ordinary consumers omit the tag. Public partition layouts stay the same. Apache 4.1.0, 4.1.2, 4.2.1, and 4.3.1 serialize the same request bytes. A01–A05 regressions run at the observed v18. Replica acknowledgement and quorum commit are outside this codec. |
