# Support matrix

Authoritative supported combinations for **partitionline 0.1.x** while the crate
is on 0.x. This is a KL-08 honesty document: it states what CI and maintainers
actually cover today. It is **not** a 1.0 support contract and does **not** lift
Suite HOLD.

For API churn rules see [api-stability.md](api-stability.md). For how cuts are
published see [RELEASE.md](RELEASE.md). For adopter steps see [ADOPTION.md](ADOPTION.md).

**Known qualification limits:** the [2026-09-21 source audit](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/audits/2026-09-21.md)
reproduced five consumer correctness cases at `cb7e97d`. CI-backed below means
the named lanes execute, not that all client semantics are correct.
Apache 4.1.2/4.2.1/4.3.1 now have separate digest-pinned, exact-history
compatibility profiles. The earlier 3.9.1/4.1.0 smoke cells remain historical
executed coverage. The current profiles qualify the scenarios described below;
the [task queue](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/plan/README.md)
still tracks broader correctness, fault and performance work.

## Supported (CI-backed)

| Dimension | Supported now | Evidence |
|---|---|---|
| Crate version | `0.1.0` on crates.io | Installable; do not re-cut `0.1.0` |
| MSRV | Rust **1.85** (`rust-version` in `Cargo.toml`) | `test (1.85)` and `test (stable)` CI; raising MSRV is a 0.x minor + CHANGELOG note |
| Host OS (CI) | Linux (`ubuntu-latest`); native macOS (`macos-15`) and Windows (`windows-2025`) for mock/runtime and packed consumers | `.github/workflows/ci.yml`; KL08-05 / KL08-14 native default/tracing cells |
| Host arch (CI) | Linux `x86_64`; macOS `arm64`; Windows `x86_64` MSVC | Native compiler/runtime guards and retained platform reports |
| Brokers (current source) | Apache Kafka **4.1.2**, **4.2.1**, **4.3.1**; historical smoke on **3.9.1** / **4.1.0** | `broker-current` required profiles and separate `broker-smoke` history; [frozen image digests](../tests/conformance/current-broker-cells.json) |
| Default features | Pure Rust (no librdkafka / OpenSSL / libzstd / Cyrus SASL) | `Cargo.toml` defaults + deny/audit lanes |
| Auth in smoke | SASL PLAIN / SCRAM / OAUTHBEARER + rustls TLS (when auth smoke runs) | `scripts/ci-auth-smoke.sh` (soft-skip without Java/Kafka unless `REQUIRE_AUTH=1`) |

KL01-10 qualifies the three current broker distributions at source `8bec4f2`
in [CI run 36869103809](https://github.com/mingley/partitionline/actions/runs/36869103809).
Each fresh Linux x86_64 single-broker RF1 cell requires admin checks, 16 exact
produced records, manual consumption, one-member classic/cooperative/KIP-848
groups, share accept/close, and transactional output/offset commit with a hidden
abort sentinel. Rust verifies complete identities, offsets, timestamps and
payloads; the pinned Java CLI independently reads both histories and verifies
four groups' committed offsets and zero lag. Actual image/compiler/API ranges
and finalized feature levels are retained; API absence or a disabled required
feature fails the profile. Transaction commit-marker visibility is observed
within five seconds without repeating the transaction. Startup coordinator
errors and every visibility observation remain in the evidence.

KL01-17 qualifies source `9404187` with retained [complete live evidence](plan/evidence/KL01-17.json).
The Apache 4.1.2 lane also requires the KL01-17
[frozen live verifiable scenario](../tests/conformance/verifiable-live-profile.json).
It runs both Rust adapter binaries on one fresh partition, checks every event
for 25 null-key decimal values, and confirms all records and the final committed
offset 25 / lag 0 with that broker's Java CLI. Each attempt retains its actual
clean source SHA, compiler/image identity, full streams and failures. This is
one producer/consumer scenario; it does not qualify the full ducktape suite or
the full SDK case registry. The earlier mock-only case remains historical.

These profiles qualify current source with default features. They do not add
multi-broker HA, rebalance churn, share lock-expiry, crash/fencing, external auth,
native macOS/Windows broker, tiered-storage or performance qualification. The
historical smoke checks do not qualify a failed current profile. Published
`0.1.0` remains a separate source baseline.

## Explicitly unsupported / not promised

KL08-05 qualifies native macOS arm64 on `macos-15`, for Rust 1.85.0
and stable, with default/tracing runtime, TLS mock paths and actual packed-crate
consumers. Both hosted toolchains passed; broker/performance qualification
remains in the Linux lanes. macOS needs Xcode command-line build tools, Python 3.11+ and Homebrew
Bash 5+ / OpenSSL 3 on PATH. Apple Bash 3.2 cannot run the package/documentation
scripts' empty arrays under `set -u`. OpenSSL generates ephemeral mock certificates
and is a test executable,
not a crate dependency. The lane records actual OS, architecture, compiler,
OpenSSL, Bash and Python versions with complete logs/package reports. Linux MSRV,
broker and performance lanes remain in place.

KL08-14 qualifies native `windows-2025` x86_64 MSVC for Rust 1.85.0 and
stable, with default/tracing runtime, mandatory public TLS mock paths and actual
packed-consumer checks. Both hosted toolchains passed at source `5b02bc5`;
Windows live-broker, external auth-service and performance campaigns remain
unqualified. Prerequisites are MSVC C build tools for Ring, native 64-bit Python
3.11+ in UTF-8 mode (`PYTHONUTF8=1`), Git Bash 5+ and OpenSSL 3 on PATH. The driver maps package scripts'
`python3` calls to setup-python's native `python.exe` and records the actual
certificate executable/version. No cross-build, WSL, skipped-runtime or
compilation-only result qualifies this cell.

Each row names its KL05-01
[feature-registry](../tests/conformance/features.json) entry where one exists;
registry status was re-checked at source `ca50ca1` (KL07-07).

| Item | Status | Registry |
|---|---|---|
| Kerberos / GSSAPI | Not in default features; no CI promise | `auth.sasl_gssapi` (`missing`) |
| zstd (C) as a default dependency | Denied / out of default features (`deny.toml` bans `zstd-sys`) | `codecs.zstd.decode/encode/wire_helper` (`missing`) |
| Schema Registry as part of this crate | Outside core; unpublished companion has bounded read-only lookups/cache and Protobuf indexes with a caller-selected codec. Built-in Avro/JSON serializers remain pending. | `schema_ecosystem.registry_client/cache/protobuf` (`present`, companion scope); `avro/json_schema` (`missing`); generic `wire_framing` (`partial`) |
| Multi-broker chaos / HA proof | KL-03 still open | `manual_consumer.fetch` (`partial`); heartbeat/throttle scheduling `partial` |
| Proactive OIDC token refresh | Not implemented | `auth.sasl_oidc_refresh` (`missing`) |
| Sticky unkeyed partitioner | Not implemented (round-robin instead) | `producer.sticky_partitioner` (`missing`) |
| Signed Suite HOLD / Lab A | **Unsigned** — Suite HOLD remains | — (qualification gate, not a feature entry) |
| macOS Intel / other host architectures | May build; no CI support promise for these cells | — (CI dimension, not a feature entry) |
| Every Kafka API version | Demand-led (KL-05); see [gaps.md](gaps.md) | `producer.v13_wire`, `manual_consumer.v18_wire` (`missing`) |

ListOffsets now supports v1–v11 with explicit KIP-1023 earliest pending upload
(`-6`, `OffsetSpec::earliest_pending_upload`). Admin and manual consumers refresh
each selected leader’s range and reject unsupported selectors/isolation before
the ListOffsets request; caller deadlines cover metadata/connect/negotiation/RPC
and retries. Independent Apache bytes and mock paths qualify the delta. No live
tiered-storage deployment or pending-upload boundary is qualified.

DescribeLogDirs now negotiates each selected broker’s v1–v5 range and exposes
typed `IsCordoned` at v5 (older versions default to false). Each attempt adds one
ApiVersions control RPC within that broker hop’s deadline. Independent Apache
4.3.1 bytes and mixed-version mock brokers qualify this delta; live v5 broker
behavior remains unqualified. Public struct literals for `DescribeLogDirsResult`
need the new `is_cordoned` field; its existing constructor defaults it to false.

Source now includes typed `elect_leaders`, read-only `describe_quorum` and
`add_raft_voter` / `remove_raft_voter` Admin methods. Both membership operations
follow broker forwarding with v0
negotiation, explicit voter/directory/cluster validation and a total deadline;
addition also validates named endpoints. Removal has no wire TimeoutMs field.
Independent Apache wire fixtures and mock failures cover its contract; no real
controller membership change or direct controller-bootstrap support is qualified.
A lost response can leave membership ambiguous: inspect the quorum before another
change. Production membership operations need an operator-approved procedure.

**Source versus published crate:** the migration map
([migrate-from-rdkafka.md](migrate-from-rdkafka.md)) was verified at source
`ca50ca1`, which postdates the packed crates.io `0.1.0` (14 files under
`src/` differ). Mapped configuration defaults are identical in both except
`ConsumerConfig::buffer_memory` (32 MiB fetch cap, current source only).

**Error and configuration contract:** the normative 0.x caller contract for
error categories (retryable, abort-required, fatal, unsupported, timeout,
ambiguous-delivery) and for public config fields (rejected combinations,
normalizations, Java default differences) is frozen in
[api-stability.md](api-stability.md) (KL07-10). Match error categories, not
`Display` text; construct configs via builders or `Default` plus overrides.

## Security response

Report vulnerabilities privately (GitHub security advisories when enabled). See
[security.md](security.md). Credential `Debug` redaction is documented there; it
does not replace rotation/outage recovery work (KL-06 open).

## Upgrade and deprecation (0.x)

- Prefer patch for fixes-only; breaking **Stable** surfaces bump `0.MINOR` (see
  [RELEASE.md](RELEASE.md) and [api-stability.md](api-stability.md)).
- Unsupported combinations above may break without a major bump while on 0.x;
  they will not be silently advertised as supported in this matrix.
- Rollback of a production cut is operator-controlled; release rehearsal covers
  partial-release recovery without publishing (`scripts/rehearse-partial-release.sh`).

## Remaining KL-08 gaps

This matrix Does **not** close KL-08. Still open: two independent adopter
24h/7d records, traffic-shadow promotion, and operator-approved rollback proof
under production SLOs. Use the blank
[adopter exercise template](adopter-exercise.md) to record runs when they
happen — the template itself is **UNFILLED** and is not evidence.

Produce quota scheduling supports negotiated v6–v12 across all connections to
one broker, while v3–v5 retain server throttling. Positive waits share the existing
delivery deadline and close budget. The fixed-size `ProducerMetrics.throttle`
snapshot reports requested time and invalid negative responses. Deterministic
peer tests cover broker isolation, in-flight acknowledgements, reconnection,
version boundaries, expiry and shutdown; no live quota deployment is claimed.
Admin and heartbeat/group quota scheduling remain separate qualifications.

Fetch quota scheduling supports negotiated v8–v17 per broker with per-response
completion clocks; v4–v7 retain server throttling. Other brokers and buffered
records continue delivering while a peer is muted. All-muted waits share the
poll long-poll and original request budgets and accept wakeup/caller cancellation.
`ConsumerMetrics.throttle` uses fixed-size requested-time counters. Deterministic
peer tests cover mixed versions, zero/negative durations, response discard at the
buffer cap, deadline expiry, cancellation and idle reconnection. The Apache Java
4.3.1 scheduling reference is executed independently; no live quota deployment
or throughput improvement is claimed.

Incremental Fetch runtime support covers broker-local v7–v17 sessions and v4–v6
full fallback, partition/identity validation, bounded session-error recovery,
assignment and topic-ID changes, and terminal close. Tests and the strict
32-partition live runner cover a named reconnect reset with 65 exact Rust/Java
records. The Apache 4.3.1 handler independently executes 80 state transitions.
Request-byte reduction is measured; no throughput, latency or sustained broker
fault campaign result is claimed.
