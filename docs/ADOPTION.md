# Adoption path

partitionline is meant to replace librdkafka-backed clients for services that
need a memory-safe Kafka stack with no C in the default feature set.

## Owner unblock (WP-0.5)

Civilization **Installable** is blocked only on credentials and merge:

1. Add `CARGO_REGISTRY_TOKEN` (Cloud Agent env + GitHub Actions secret).
2. Cancel stale tip/tag Actions still stuck in `queued` (agents get 403).
   **Owner:** `bash scripts/owner-cancel-stuck-runs.sh` (or `DRY_RUN=1` first).
   `main` CI is green again (e.g. run `33714516185`). Tip (`dev/**`) pushes
   no longer auto-queue CI (was thrashing starved runners); local gate:
   `bash scripts/ci-branch-lite.sh`. Full matrix on PR/`main`/`workflow_dispatch`.
   Local civilization-check includes broker + SASL_SSL PLAIN + SCRAM-256/512 +
   OAUTHBEARER + OIDC + mTLS. After merge, `ci.yml` cancels in-progress on
   `main` so the next push can clear a stuck predecessor.
3. Merge `dev/civilization-plan-b686` → `main`, tag **`v0.1.0`** (final only —
   not `-rc`; `release.yml` ignores prerelease tags), confirm
   https://crates.io/crates/partitionline (`bash scripts/check-installable.sh`
   should exit 0).
4. Run `bash scripts/day1-after-publish.sh` and commit the README crates.io line.

Probe current blockers anytime:

```bash
bash scripts/owner-status.sh
# One-shot checklist (status + dry-run cancel + merge/tag/day1 path):
bash scripts/owner-unblock.sh
# When Actions stay queued, local Verifiable proxy:
bash scripts/ci-branch-lite.sh
```

## Install (today)

crates.io publish is the remaining owner step (`docs/RELEASE.md`). Until the
first release lands, pin a **tag** (not floating `main`):

```toml
[dependencies]
partitionline = { git = "https://github.com/mingley/partitionline", tag = "v0.1.0-rc.5" }
```

`v0.1.0-rc.5` tracks the civilization tip (rc.4 plus `scripts/check-adopter-pin.sh` so git pins cannot silently lag tip). Prefer this over floating `main`. After crates.io `0.1.0`, switch to:

```toml
[dependencies]
partitionline = "0.1"
```

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

## Known adoption gaps (honest)

| Gap | Status |
|---|---|
| crates.io release | Owner token + tag `v0.1.0` (WP-0.5) |
| zstd compression | Out of default features (C); see `docs/zstd-spike.md` |
| Kerberos / GSSAPI | Out of default features (C) |
| Schema Registry | Companion after crates.io; design in `docs/schema-companion.md` |
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

Local evidence (2026-09-04): `ci-civilization-check.sh` **26/26** including
native broker smoke and SASL_SSL PLAIN + SCRAM + OAUTHBEARER + OIDC + mTLS auth smoke. GitHub Actions
still cannot confirm Verifiable until org runners leave `queued`.