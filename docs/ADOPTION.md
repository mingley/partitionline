# Adoption path

partitionline is meant to replace librdkafka-backed clients for services that
need a memory-safe Kafka stack with no C in the default feature set.

## Owner unblock (WP-0.5)

Civilization **Installable** is blocked only on credentials. Probe anytime:

```bash
bash scripts/check-installable-preflight.sh   # READY_EXCEPT_TOKEN when cut-ready
bash scripts/owner-status.sh
bash scripts/owner-unblock.sh                 # status + dry-run cancel + finish path
```

1. Add `CARGO_REGISTRY_TOKEN` (Cloud Agent env + GitHub Actions secret).
2. Cancel stale tip/tag Actions still stuck in `queued` (agents get 403).
   **Owner:** `bash scripts/owner-cancel-stuck-runs.sh` (or `DRY_RUN=1` first).
   `main` HEAD CI is green (e.g. run `33850540606` on `6431785` — Kafka
   3.9.1 + 4.1.0 broker-smoke + latency-gate). Tip (`dev/**`) pushes no longer
   auto-queue CI; local gate: `bash scripts/ci-branch-lite.sh`. Full matrix on
   PR/`main`/`workflow_dispatch`. While the token is missing, leave tip ahead
   on docs/scripts — `owner-sync-main` refuses docs-only tip→main thrash
   (restarting broker-smoke cancels in-flight Verifiable).
3. **Preferred once the token is in-env** (bypasses starved Actions for the
   first cut):
   ```bash
   bash scripts/owner-finish-installable.sh

After Installable lands, merge parked Verifiable work (`dev/verifiable-auth-integrity-fuzz-b686`: Actions `auth-smoke` + `integrity-smoke`, ConsumerGroupHeartbeat fuzz). Kept off tip so the token cut stays docs/scripts-only / one-shot `PUBLISH_LOCAL`.
   ```
   Fast-forwards tip → `main` once (if tip is ahead), publishes locally,
   runs day1, and proves Installable. Or stepwise:
   `bash scripts/owner-cut-release.sh` (tags **`v0.1.0`** final only).
   If the token is **Actions-only** (not in your shell): cancel stuck runs,
   then Actions → **First publish** → `confirm=publish` or
   `bash scripts/owner-dispatch-first-publish.sh`
   (`.github/workflows/first-publish.yml` is already on `main`).
4. Commit the README crates.io line if day1 changed it; configure crates.io
   Trusted Publishing for `release.yml`.

## Install (today)

crates.io publish is the remaining owner step (`docs/RELEASE.md`). Until the
first release lands, pin a **tag** (not floating `main`):

```toml
[dependencies]
partitionline = { git = "https://github.com/mingley/partitionline", tag = "v0.1.0-rc.6" }
```

`v0.1.0-rc.6` tracks the civilization tip (rc.5 plus owner-publish→day1 chaining and refreshed native Kafka 4.1 broker smoke including kip848/share). Prefer this over floating `main`. After crates.io `0.1.0`, switch to:

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
native broker smoke and SASL_SSL PLAIN + SCRAM + OAUTHBEARER + OIDC + mTLS auth
smoke. Same-day recheck: `SKIP_DOCKER=1` broker-smoke (kip848+share),
`REQUIRE_AUTH=1` auth-smoke, integrity COUNT=2000 HW==acked+consumed==seeded,
latency gate p99≈71–86µs vs 500µs baseline — all unsigned; not a Suite HOLD
lift. Later same day: local `KAFKA_IMAGE=apache/kafka:4.1.0` broker-smoke
(kip848+share) green after the Docker `KAFKA_*` / `process.roles` fix; Actions
`latency-gate` on `main` @ `910015f` also green (`LATENCY_LIMIT_US=5000`);
Actions `broker-smoke` Kafka **4.1.0** green on that same SHA; Kafka **3.9.1**
needed a soft-skip for optional kip848 `Protocol` truncate (`7051625` re-run
`33848465892` in flight). Packed-crate downstream consumer gate green tip-side.