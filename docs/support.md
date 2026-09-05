# Support matrix

Authoritative supported combinations for **partitionline 0.1.x** while the crate
is on 0.x. This is a KL-08 honesty document: it states what CI and maintainers
actually cover today. It is **not** a 1.0 support contract and does **not** lift
Suite HOLD.

For API churn rules see [api-stability.md](api-stability.md). For how cuts are
published see [RELEASE.md](RELEASE.md). For adopter steps see [ADOPTION.md](ADOPTION.md).

## Supported (CI-backed)

| Dimension | Supported now | Evidence |
|---|---|---|
| Crate version | `0.1.0` on crates.io | Installable; do not re-cut `0.1.0` |
| MSRV | Rust **1.85** (`rust-version` in `Cargo.toml`) | `cargo` CI on stable; raising MSRV is a 0.x minor + CHANGELOG note |
| Host OS (CI) | Linux (GitHub Actions `ubuntu-latest`) | `.github/workflows/ci.yml` |
| Host arch (CI) | `x86_64` | Actions runners |
| Brokers | Apache Kafka **3.9.1** and **4.1.0** (`apache/kafka:3.9.1`, `apache/kafka:4.1.0`) | `broker-smoke` matrix (`KAFKA_IMAGE`) |
| Default features | Pure Rust (no librdkafka / OpenSSL / libzstd / Cyrus SASL) | `Cargo.toml` defaults + deny/audit lanes |
| Auth in smoke | SASL PLAIN / SCRAM / OAUTHBEARER + rustls TLS (when auth smoke runs) | `scripts/ci-auth-smoke.sh` (soft-skip without Java/Kafka unless `REQUIRE_AUTH=1`) |

## Explicitly unsupported / not promised

| Item | Status |
|---|---|
| Kerberos / GSSAPI | Not in default features; no CI promise |
| zstd (C) as a default dependency | Denied / out of default features |
| Schema Registry as part of this crate | Companion design only (`partitionline-schema` not published) |
| Multi-broker chaos / HA proof | KL-03 still open |
| Signed Suite HOLD / Lab A | **Unsigned** — Suite HOLD remains |
| Windows / macOS / non-x86_64 as CI-guaranteed | May build; not a CI matrix promise today |
| Every Kafka API version | Demand-led (KL-05); see [gaps.md](gaps.md) |

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
[adopter exercise template](adopter-exercise.md) and
[promotion/rollback exercise template](promotion-rollback-exercise.md) to
record runs when they happen — both templates ship **UNFILLED** and are not
evidence.
