# Support matrix

What CI actually runs for partitionline 0.1.x, and what it does not promise.
A row here means that lane executes. It does not mean every client behavior is
qualified.

API changes follow the [0.x policy](api-stability.md). Cuts are described in
[RELEASE.md](RELEASE.md). Adopter steps are in [ADOPTION.md](ADOPTION.md).
Broader correctness, fault, and performance work stays in the
[task queue](plan/README.md).

The [2026-09-21 source audit](audits/2026-09-21.md) reproduced five consumer
correctness cases at `cb7e97d`. Those cases are fixed in later source.
Apache 4.1.2, 4.2.1, and 4.3.1 have their own digest-pinned profiles. The
older 3.9.1 and 4.1.0 smoke runs are historical coverage.

## Supported (CI-backed)

| Dimension | Supported now | Evidence |
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
Status comes from the current registry. Platform and production qualification
stay separate.

| Item | Status | Registry |
|---|---|---|
| Kerberos / GSSAPI | Not in default features; no CI promise | `auth.sasl_gssapi` (`missing`) |
| zstd | Optional bounded encoding/decoding with `zstd-rs`; default dependencies unchanged | `codecs.zstd.decode/encode/wire_helper` (`present`, finite qualification) |
| Schema Registry as part of this crate | Outside core; unpublished companion has bounded read-only lookups/cache and Protobuf indexes with a caller-selected codec. Applications select Avro/JSON serializers explicitly. | `schema_ecosystem.registry_client/cache/protobuf` (`present`, companion scope); `avro/json_schema` and generic `wire_framing` (`present`, bounded offline qualification) |
| Multi-broker chaos / HA proof | KL-03 still open | `manual_consumer.fetch` (`partial`); heartbeat/throttle scheduling `partial` |
| OIDC token lifecycle | Application-owned `OidcTokenManager` supports refresh; shared connection-open caching has bounded idle retention. Broader connection renewal qualification remains separate. | `auth.sasl_oidc_refresh`; see [security](security.md#token-acquisition-and-lifecycle) |
| Sticky unkeyed partitioner | Not implemented (round-robin instead) | `producer.sticky_partitioner` (`missing`) |
| Signed Suite HOLD / Lab A | **Unsigned** — Suite HOLD remains | — (qualification gate, not a feature entry) |
| macOS Intel / other host architectures | May build; no CI support promise for these cells | — (CI dimension, not a feature entry) |
| Every Kafka API version | Demand-led (KL-05); see [gaps.md](gaps.md) | `producer.v13_wire` (`present`); `manual_consumer.v18_wire` (`present`) |

## What recent source adds

These notes describe the working source. They do not by themselves qualify a live multi-broker deployment.

### Produce topic IDs

Produce v13 topic IDs are checked against independent Apache 4.1.2, 4.2.1, and 4.3.1 serialization, live Java decoding of fresh Rust requests and responses, and deterministic peers for topic recreation, immutable response identity, missing IDs, delivery-budget retries, and the v3–v12 fallback. Transaction version 1 stays on Produce v11. Finalized `transaction.version` of 2 or higher, with EndTxn v5, enables v12 and v13. That choice is fixed when the producer starts.

### ListOffsets

ListOffsets supports v1–v11, including KIP-1023 earliest pending upload (`-6`, `OffsetSpec::earliest_pending_upload`). Admin and manual consumers refresh each selected leader’s range and reject an unsupported selector or isolation level before the request. The caller deadline covers metadata, connect, negotiation, the RPC, and retries. Independent Apache bytes and mock paths cover this delta. No live tiered-storage deployment or pending-upload boundary is qualified.

### ListTransactions

ListTransactions supports v0–v2, with a complete listing and a per-broker listing. Typed options keep the duration filter and the transaction-ID pattern. A nonempty pattern requires v2. It is not dropped on an older broker. Three Apache SDKs check selected wire and public-call behavior against bounded scripted peers. Live transaction-state recovery is separate work.

### Metadata and coordinator discovery

Metadata and GROUP coordinator discovery include v0. A named v0 metadata request requires automatic topic creation. Unsupported options are refused. Missing controller, UUID, and rack fields keep their sentinel values. This wire support differs from the current Java Metadata builder, which refuses v0.

### Offset commit and fetch

OffsetCommit and OffsetFetch support v10 topic UUIDs on typed requests, on `ConsumerGroup`, and on Admin. Name-based calls keep the older wire formats. A UUID-only request refuses an older coordinator. Metadata snapshots and assignment IDs keep offsets tied to the original topic when a topic of the same name is recreated. Selected checks use three pinned Apache SDKs and scripted peers. Live coordinator persistence and cluster failover are not qualified.

### DescribeLogDirs

DescribeLogDirs negotiates each selected broker’s v1–v5 range. Typed `IsCordoned` appears at v5. Older versions default it to false. Each attempt adds one ApiVersions control RPC inside that broker hop’s deadline. Independent Apache 4.3.1 bytes and mixed-version mock brokers cover this delta. Live v5 broker behavior is not qualified. A public struct literal for `DescribeLogDirsResult` needs the `is_cordoned` field. The existing constructor defaults it to false.

### Streams

Streams v0 codecs and caller-driven heartbeats keep typed topology, tasks, offsets, endpoints, and status fields. Heartbeats use GROUP coordinator discovery, the connection’s actual capabilities, and one caller deadline. v0 is explicit. Three pinned Apache SDKs check selected public Rust socket histories and typed responses. This client does not include a Streams execution engine, and the broker does not include a Streams coordinator. Typed Admin descriptions keep every group, member, and topology field, including caller order and duplicates. Discovery or capability failures are reported per group.

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

The migration map ([migrate-from-rdkafka.md](migrate-from-rdkafka.md)) was verified at source `ca50ca1`, which is later than the packed crates.io `0.1.0` (14 files under `src/` differ). Mapped configuration defaults match, except `ConsumerConfig::buffer_memory` (a 32 MiB fetch cap, current source only).

The 0.x caller contract for error categories (retryable, abort-required, fatal, unsupported, timeout, ambiguous delivery) and for public config fields is in [api-stability.md](api-stability.md). Match error categories, not `Display` text. Build configs with the builders, or with `Default` plus overrides.

Report vulnerabilities privately. See [security.md](security.md). Credential `Debug` redaction is documented there. It does not replace the live credential-lifecycle matrix (KL06-06), which is still pending.

### Upgrades

Prefer a patch release for fixes. A breaking change to a Stable surface bumps `0.MINOR`. See [RELEASE.md](RELEASE.md) and [api-stability.md](api-stability.md). Combinations listed above as unsupported may change while the crate is on 0.x. They will not be advertised as supported in this matrix. Rolling back a production cut is the operator’s job. Release rehearsal covers partial-release recovery without publishing (`scripts/rehearse-partial-release.sh`).

### Still open for KL-08

This matrix does not close KL-08. Still open: two independent adopter 24-hour and 7-day records, traffic-shadow promotion, and an operator-approved rollback proof under production SLOs. The [adopter exercise template](adopter-exercise.md) is blank. Filling it in is how a run becomes evidence. The template itself is not evidence.

### Quotas and incremental fetch

Produce quota scheduling supports negotiated v6–v13 on every connection to one broker. v3–v5 keep server throttling. Positive waits share the existing delivery deadline and close budget. The fixed-size `ProducerMetrics.throttle` snapshot reports requested time and an invalid negative response. Deterministic peer tests cover broker isolation, in-flight acknowledgements, reconnection, version boundaries, expiry, and shutdown. No live quota deployment is claimed. Admin and heartbeat or group quota scheduling are separate.

Fetch quota scheduling supports negotiated v8–v18 per broker, with a completion clock on each response. v4–v7 keep server throttling. Other brokers and buffered records keep delivering while one peer is muted. An all-muted wait shares the poll long-poll and the original request budget, and it accepts wakeup and caller cancellation. `ConsumerMetrics.throttle` uses fixed-size requested-time counters. Deterministic peer tests cover mixed versions, zero and negative durations, discarding a response at the buffer cap, deadline expiry, cancellation, and idle reconnection. The Apache Java 4.3.1 scheduling reference is executed separately. No live quota deployment or throughput improvement is claimed.

Incremental Fetch covers broker-local sessions on v7–v18 and a full fetch fallback on v4–v6, plus partition and identity checks, bounded session-error recovery, assignment and topic-ID changes, and a terminal close. Tests and the strict 32-partition live runner cover a named reconnect reset with 65 exact Rust and Java records. The Apache 4.3.1 handler executes 80 state transitions on its own. Request-byte reduction is measured. No throughput, latency, or sustained broker-fault result is claimed.

Fetch v18 (KIP-1166) carries replica HighWatermark sidecars, with strict lengths for known tags. Ordinary consumers omit the tag. Existing public partition layouts and helper signatures stay the same. Apache 4.1.0, 4.1.2, 4.2.1, and 4.3.1 serialize the same request bytes. Named A01–A05 regressions run at the observed v18. Replica acknowledgement and quorum commit are outside this client codec.
