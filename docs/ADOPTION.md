# Adoption path

partitionline is meant to replace librdkafka-backed clients for services that
need an asynchronous Kafka client with typed configuration and bounded queues.

Read the [support matrix](support.md) before choosing a version. It lists the
tested Kafka versions, Rust toolchains, platforms, and qualification limits.

Version 0.1.0 is published. Check the package with
`bash scripts/check-installable.sh`. Maintainers planning another release should
use the [release guide](RELEASE.md); earlier release records are in
[STATUS.md](STATUS.md).

## Install

`partitionline` **0.1.0** is on [crates.io](https://crates.io/crates/partitionline):

```toml
[dependencies]
partitionline = "0.1"
```

Git tags remain available for bisects; new adopters should prefer crates.io.


## Pilot checklist

1. Produce + fetch against your Kafka 3.9 / 4.x cluster (`examples/roundtrip`).
2. Classic, cooperative, or KIP-848 groups (`examples/group`, `examples/cooperative`, `examples/kip848` on Kafka 4.x).
3. TLS (`rustls`, optional mTLS via `TLS_CLIENT_CERT_PEM` /
   `TLS_CLIENT_KEY_PEM` on `examples/tls`) and SCRAM-SHA-256/512 or OIDC as
   required (`examples/sasl` with `TLS_CA_PEM` for SASL_SSL, `examples/oauth`).
   Real-broker check: `REQUIRE_AUTH=1 bash scripts/ci-auth-smoke.sh`
   (PLAIN + SCRAM + OAUTHBEARER + OIDC + mTLS).
4. Transactions / EOS if you need them (`examples/txn`, `examples/eos`).
5. Share groups on Kafka 4.1+ with `share.version=1` (`examples/share`).
6. Scrape `Producer` / `Consumer` / `Admin` / `ShareGroup` metrics; optional
   `tracing` feature for spans (`docs/guide.md`).
7. Read defaults that differ from Java (`auto.offset.reset=Earliest`, etc.).

For KL-08 **24h then 7d** adopter evidence (two independent workloads), copy
[adopter-exercise.md](adopter-exercise.md). The template ships **UNFILLED** and
does not close KL-08 or lift Suite HOLD until real records are filed.

## Remaining work

| Gap | Status |
|---|---|
| crates.io release | **0.1.0 published** — `partitionline = "0.1"` |
| zstd compression | Client support unfinished; see [backend decision](zstd-spike.md) |
| Kerberos / GSSAPI | Client support unfinished |
| Schema Registry | Separate unpublished [companion](https://github.com/mingley/partitionline/blob/8a50e8d18df40787d86eb714ff363b9d1e41ce43/partitionline-schema/README.md) with read-only lookups, caching, and serializer adapters |
| Signed Suite HOLD benches | External Lab A process (`docs/STATUS.md`) |

Tell us what blocks a pilot: issue
[#85](https://github.com/mingley/partitionline/issues/85) or the adoption
issue template.

## Verify locally

```bash
bash scripts/ci-civilization-check.sh
# Docker-less agent / nested VM:
bash scripts/ci-native-kafka.sh start
SKIP_DOCKER=1 bash scripts/ci-broker-smoke.sh
bash scripts/ci-native-kafka.sh stop
# TLS + SCRAM (isolated ports; needs local Kafka/Java/openssl):
REQUIRE_AUTH=1 bash scripts/ci-auth-smoke.sh   # PLAIN + SCRAM + OAUTHBEARER + OIDC + mTLS
```

See [STATUS.md](STATUS.md) for earlier runs and [benchmark instructions](benchmark.md)
for current measurement procedures.

## Dependabot vs post-cut parks

Dependabot PRs for flate2 1.1.10 and SCRAM crypto (hmac/pbkdf2/sha2) overlap
parked `dev/scram-crypto-bumps-b686`; `lz4_flex` 0.14 is parked on
`dev/lz4-flex-bump-b686`; `actions/checkout` v7 is parked on
`dev/actions-checkout-bump-b686`. **Post-cut parks for these bumps have landed on `main`.** Prefer closing the
overlapping Dependabot PRs once `check-parks-on-main` is OK. New bumps go to
`main` (tip absorbs). Historical pre-cut rule: do not merge bumps onto tip while
Installable waited — that broke docs/scripts-only tip-delta. Landed via
`bash scripts/owner-land-post-cut-parks.sh` / finish's default chain; close
overlapping Dependabot PRs (#87 lz4_flex, #88–#91 sha2/flate2/pbkdf2/hmac, #92 actions/checkout).

Executable gate (wired into cut-path + bars + actions hygiene):

```bash
bash scripts/check-dependabot-parks-coverage.sh
```

Unmapped open Dependabot cargo/Actions bumps fail the check so stewardship cannot
drift. Post-Installable, land bumps on `main` (tip absorbs main) — do not reopen
pre-cut tip-only parks for bumps already on main.
