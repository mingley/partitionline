# Adoption path

partitionline is meant to replace librdkafka-backed clients for services that
need a memory-safe Kafka stack with no C in the default feature set.

## Owner unblock (WP-0.5)

Civilization **Installable** is blocked only on credentials and merge:

1. Add `CARGO_REGISTRY_TOKEN` (Cloud Agent env + GitHub Actions secret).
2. Restore GitHub Actions runners — **org-wide**, not just this branch: `main`
   has had a run stuck `queued` for ~22h, and `dev/**` `branch-lite` also
   never leaves the queue. Agent cannot cancel runs (403).
3. Merge `dev/civilization-plan-b686` → `main`, tag `v0.1.0` (or
   `workflow_dispatch` on that tag), confirm https://crates.io/crates/partitionline
   (`bash scripts/check-installable.sh` should exit 0).
4. Run `bash scripts/day1-after-publish.sh` and commit the README crates.io line.

## Install (today)

crates.io publish is the remaining owner step (`docs/RELEASE.md`). Until the
first release lands:

```toml
[dependencies]
partitionline = { git = "https://github.com/mingley/partitionline" }
```

After `v0.1.0`:

```toml
[dependencies]
partitionline = "0.1"
```

## Pilot checklist

1. Produce + fetch against your Kafka 3.9 / 4.x cluster (`examples/roundtrip`).
2. Classic or cooperative groups (`examples/group`, `examples/cooperative`).
3. TLS (`rustls`) and SCRAM or OIDC as required (`examples/tls`, `examples/sasl`).
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
```
