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
Apache 4.1.2/4.2.1/4.3.1 are planned compatibility targets, not covered by the
existing 3.9.1/4.1.0 matrix. Follow [the task queue](https://github.com/mingley/partitionline/blob/917d877d7b049f3da5af90bd2a5804b85080ed2b/docs/plan/README.md) for repairs
and evidence before extending support claims.

## Supported (CI-backed)

| Dimension | Supported now | Evidence |
|---|---|---|
| Crate version | `0.1.0` on crates.io | Installable; do not re-cut `0.1.0` |
| MSRV | Rust **1.85** (`rust-version` in `Cargo.toml`) | `test (1.85)` and `test (stable)` CI; raising MSRV is a 0.x minor + CHANGELOG note |
| Host OS (CI) | Linux (`ubuntu-latest`); native macOS (`macos-15`) for mock/runtime and packed consumers | `.github/workflows/ci.yml`; KL08-05 native default/tracing cells |
| Host arch (CI) | Linux `x86_64`; macOS `arm64` | Native compiler/runtime guards and retained platform reports |
| Brokers | Apache Kafka **3.9.1** and **4.1.0** (`apache/kafka:3.9.1`, `apache/kafka:4.1.0`) | `broker-smoke` matrix (`KAFKA_IMAGE`) |
| Default features | Pure Rust (no librdkafka / OpenSSL / libzstd / Cyrus SASL) | `Cargo.toml` defaults + deny/audit lanes |
| Auth in smoke | SASL PLAIN / SCRAM / OAUTHBEARER + rustls TLS (when auth smoke runs) | `scripts/ci-auth-smoke.sh` (soft-skip without Java/Kafka unless `REQUIRE_AUTH=1`) |

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
broker and performance lanes remain in place; this card adds no Windows promise.

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
| Windows / macOS Intel / other host architectures | May build; no CI support promise for these cells | — (CI dimension, not a feature entry) |
| Every Kafka API version | Demand-led (KL-05); see [gaps.md](gaps.md) | `full_admin.describe_log_dirs_v5`, `producer.v13_wire`, `manual_consumer.v18_wire/list_offsets_v11` (`missing`) |

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
