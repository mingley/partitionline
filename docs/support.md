# Support matrix

Authoritative supported combinations for **partitionline 0.1.x** while the crate
is on 0.x. This is a KL-08 honesty document: it states what CI and maintainers
actually cover today. It is **not** a 1.0 support contract and does **not** lift
Suite HOLD.

For API churn rules see [api-stability.md](api-stability.md). For how cuts are
published see [RELEASE.md](RELEASE.md). For adopter steps see [ADOPTION.md](ADOPTION.md).

**Known qualification limits:** the [2026-09-21 source audit](audits/2026-09-21.md)
reproduced five consumer correctness cases at `cb7e97d`. CI-backed below means
the named lanes execute, not that all client semantics are correct.
Apache 4.1.2/4.2.1/4.3.1 are planned compatibility targets, not covered by the
existing 3.9.1/4.1.0 matrix. Follow [the task queue](plan/README.md) for repairs
and evidence before extending support claims.

## Supported (CI-backed)

| Dimension | Supported now | Evidence |
|---|---|---|
| Crate version | `0.1.0` on crates.io | Installable; do not re-cut `0.1.0` |
| MSRV | Rust **1.85** (`rust-version` in `Cargo.toml`) | `test (1.85)` and `test (stable)` CI; raising MSRV is a 0.x minor + CHANGELOG note |
| Host OS (CI) | Linux (GitHub Actions `ubuntu-latest`) | `.github/workflows/ci.yml` |
| Host arch (CI) | `x86_64` | Actions runners |
| Brokers | Apache Kafka **3.9.1** and **4.1.0** (`apache/kafka:3.9.1`, `apache/kafka:4.1.0`) | `broker-smoke` matrix (`KAFKA_IMAGE`) |
| Default features | Pure Rust (no librdkafka / OpenSSL / libzstd / Cyrus SASL) | `Cargo.toml` defaults + deny/audit lanes |
| Auth in smoke | SASL PLAIN / SCRAM / OAUTHBEARER + rustls TLS (when auth smoke runs) | `scripts/ci-auth-smoke.sh` (soft-skip without Java/Kafka unless `REQUIRE_AUTH=1`) |

## Explicitly unsupported / not promised

Each row names its KL05-01
[feature-registry](../tests/conformance/features.json) entry where one exists;
registry status was re-checked at source `ca50ca1` (KL07-07).

| Item | Status | Registry |
|---|---|---|
| Kerberos / GSSAPI | Not in default features; no CI promise | `auth.sasl_gssapi` (`missing`) |
| zstd (C) as a default dependency | Denied / out of default features (`deny.toml` bans `zstd-sys`) | `codecs.zstd.decode/encode/wire_helper` (`missing`) |
| Schema Registry as part of this crate | Companion design only (`partitionline-schema` not published) | `schema_ecosystem.registry_client/cache/avro/protobuf/json_schema` (`missing`); `wire_framing` (`partial`) |
| Multi-broker chaos / HA proof | KL-03 still open | `manual_consumer.fetch` (`partial`); heartbeat/throttle scheduling `partial` |
| Proactive OIDC token refresh | Not implemented | `auth.sasl_oidc_refresh` (`missing`) |
| Sticky unkeyed partitioner | Not implemented (round-robin instead) | `producer.sticky_partitioner` (`missing`) |
| Signed Suite HOLD / Lab A | **Unsigned** — Suite HOLD remains | — (qualification gate, not a feature entry) |
| Windows / macOS / non-x86_64 as CI-guaranteed | May build; not a CI matrix promise today | — (CI dimension, not a feature entry) |
| Every Kafka API version | Demand-led (KL-05); see [gaps.md](gaps.md) | `full_admin.elect_leaders/describe_quorum/add_raft_voter/remove_raft_voter/describe_log_dirs_v5`, `producer.v13_wire`, `manual_consumer.v18_wire/list_offsets_v11` (`missing`) |

**Source versus published crate:** the migration map
([migrate-from-rdkafka.md](migrate-from-rdkafka.md)) was verified at source
`ca50ca1`, which postdates the packed crates.io `0.1.0` (14 files under
`src/` differ). Mapped configuration defaults are identical in both except
`ConsumerConfig::buffer_memory` (32 MiB fetch cap, current source only).

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
