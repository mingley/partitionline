# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to the 0.x policy in [`docs/RELEASE.md`](docs/RELEASE.md).

## [Unreleased]

### Added

- `scripts/lib/crates-io.sh`: shared Installable probe (crates.io API + sparse
  index fallback; required User-Agent — CDN returns empty 403 without one).
  Wired into `check-installable`, `check-merge-ready`, and `owner-status`.
- `scripts/check-merge-ready.sh`: `git merge-tree --write-tree` conflict probe
  vs `origin/main` (flush-left `changed in both` fallback; avoids doc/script
  false positives); owner next steps prefer `owner-cut-release.sh`.
- `scripts/check-actions-hygiene.sh`: classifies stale queued Actions (RC-tag
  release zombies vs tip CI leftovers); wired into `owner-status` /
  `owner-unblock`. `owner-cancel-stuck-runs` labels the same classes.
- `scripts/ci-crate-consumer.sh`: packed-crate downstream check now links the
  operator surface (Producer/Consumer/ConsumerGroup/ShareGroup/Admin +
  SASL/TLS config), not only Producer.
- `scripts/ci-civilization-check.sh`: Installable section uses shared crates.io
  probe, day1 README `DRY_RUN` preflight, merge-ready gate, and actions hygiene.

- `examples/kip848`: KIP-848 next-gen consumer group join (`ConsumerGroup::join_consumer_topics`).
  Broker smoke runs it on Kafka 4.x (`REQUIRE_KIP848=1` default). Join now sends an
  **empty** `TopicPartitions` array (null was rejected: "must be empty when (re-)joining").
  ConsumerGroupHeartbeat decode accepts truncated error bodies that omit trailing
  tagged fields (Kafka 4.x `INVALID_REQUEST` path).

- Auth smoke covers **OIDC client_credentials** (local `scripts/oidc-token-stub.py`)
  and **mTLS** (SSL listener with `ssl.client.auth=required`; `examples/tls`
  accepts `TLS_CLIENT_CERT_PEM` / `TLS_CLIENT_KEY_PEM`). X.509 v3 client certs
  required for rustls. Wired into `scripts/ci-auth-smoke.sh` / civilization-check.

- `scripts/ci-auth-smoke.sh`: native Kafka SASL_SSL + SCRAM-SHA-256/512 +
  OAUTHBEARER (unsecured JWT) smoke (private CA, isolated ports,
  `examples/sasl` / `examples/oauth` with `TLS_CA_PEM`); TLS-only produce must
  fail closed. Wired into `scripts/ci-civilization-check.sh`.
- `examples/sasl` and `examples/oauth` accept optional `TLS_CA_PEM` /
  `TLS_SERVER_NAME` for the production SASL_SSL path.
- `scripts/owner-status.sh` prints Installable/Verifiable blocker status
  (token, crates.io, Actions tip/main); wired into civilization-check and
  publish-ready as an informational footer.
- CI concurrency cancels superseded runs on `main` too, so a stuck queued
  `main` job cannot permanently block the next push after merge.
- `scripts/owner-cancel-stuck-runs.sh` cancels queued Actions runs older than
  15 minutes (`DRY_RUN=1` supported); wired from `owner-status` / ADOPTION for
  the org-wide runner starvation case (agents get 403 — owner must run it).
- `scripts/owner-unblock.sh` one-shot owner checklist: status probe, dry-run
  cancel targets, merge → tag `v0.1.0` → day1 path after token + runners.
- `scripts/lab-a-common.sh` shared broker helpers; `scripts/lab-a-integrity.sh`
  combined produce→HW→fetch integrity; `scripts/ci-integrity-smoke.sh` small-
  count local/CI smoke (+ unsigned latency) wired into civilization-check.
  `lab-a-fetch.sh` now also requires HW delta == acked.
- `tests/fuzz_decode_smoke.rs` also hammers group / share / txn response
  decoders (not only fetch/produce/metadata). `scripts/ci-branch-lite.sh` and
  civilization-check now **run** that smoke (tip Verifiable no longer
  `--lib`-only); optional short libFuzzer smoke when nightly+g++ are present.
- CI: `dev/**` tip pushes no longer auto-queue (org runner starvation /
  perpetual tip re-queue); full matrix on PR/`main`/`workflow_dispatch` only.
- Git install pin `v0.1.0-rc.6` (README / ADOPTION / migrate guide) for
  adopters before crates.io; does not trigger `release.yml` (final `vX.Y.Z`
  only). Advances `v0.1.0-rc.5` with `owner-publish` → `day1-after-publish`
  chaining and a refreshed native Kafka 4.1 broker smoke (kip848/share).
- `scripts/check-adopter-pin.sh` (wired into branch-lite / publish-ready /
  owner-status) so git pins cannot silently lag tip.
- `scripts/post-publish-readme.sh` `DRY_RUN=1` preflight (wired into
  `ci-publish-ready`) so day-1 README flip cannot silently break before
  crates.io exists. `owner-unblock` points at release issue #86 and the
  `v0.1.0-rc.6` interim git pin.

### Changed

- `scripts/owner-cut-release.sh` one-shot owner cut on clean `main` (tag → Actions publish → crates.io wait → day1; `PUBLISH_LOCAL=1` / `DRY_RUN=1`). Wired into `owner-unblock` and RELEASE.md.
- `ci-publish-ready` and `owner-status` also run `check-merge-ready` so the
  merge→tag path is visible in the same probes as Installable/Verifiable.
- Tip re-verified auth smoke (SASL_SSL PLAIN+SCRAM+OAUTHBEARER+OIDC + mTLS fail-closed) on 2026-09-04.
- Tip re-verified native Kafka 4.1 broker smoke (`ci-native-kafka` + kip848/share required) on 2026-09-04.
- `scripts/check-merge-ready.sh` (wired into `owner-unblock`, documented in
  RELEASE.md) gates civilization → main → `vX.Y.Z` without requiring a token:
  final version + CHANGELOG section, release.yml OIDC/secret auth, workflow
  YAML, adopter pin, and tip-vs-`main` ancestry (`FULL=1` adds branch-lite).
- `release.yml` prefers crates.io **Trusted Publishing** (OIDC via
  `rust-lang/crates-io-auth-action`, `id-token: write`) and falls back to
  `CARGO_REGISTRY_TOKEN` for the first publish. RELEASE.md / owner-unblock /
  day1 document the post-0.1.0 migration off long-lived Actions secrets.
- Tip Verifiable gates (`ci-branch-lite` / `ci-publish-ready`) run
  `scripts/check-workflows.sh` so invalid workflow YAML (the flush-left
  `run: |` class that empty-failed `release` on branch pushes) cannot regress
  unnoticed. `owner-status` prefers Actions runs for the exact HEAD SHA.
- `release.yml` hardens the crates.io publish trigger: fix invalid YAML in the
  GitHub Release notes step (flush-left multiline string caused empty-job
  `release` failures on branch pushes), input-safe concurrency / checkout
  expressions, job-level skip unless final tag or `workflow_dispatch`, and an
  explicit `X.Y.Z` tag shape check. Still publishes on `v0.1.0`.
- `scripts/post-publish-readme.sh` also inserts crates.io + docs.rs badges on
  day-1 README flip (`DRY_RUN=1` asserts badges). `release.yml` creates a
  GitHub Release after a successful crates.io publish.
- `scripts/check-adopter-pin.sh` allows docs/scripts tip drift without a new rc;
  library/`Cargo.toml` changes since the pin still fail the gate.

- `scripts/owner-publish.sh` runs `day1-after-publish` by default after a
  successful manual `cargo publish` (`RUN_DAY1_AFTER_PUBLISH=0` to skip).
- Migration guide git pin → `v0.1.0-rc.6`; guide documents KIP-848 empty
  `TopicPartitions` join + `examples/kip848`. Cargo.toml lists remaining
  examples (`share`, `cooperative`, `metrics`, `offsets`, `pause`, `wakeup`)
  so the packaged crate surface matches README.
- ADOPTION owner unblock: `main` Actions recovered; remaining cancel targets
  are stale tip/tag queues (not an eternal `main` queue).

- Mock TLS fixtures use the `openssl` CLI instead of `rcgen`, dropping
  `time` from the dependency graph and clearing the `RUSTSEC-2026-0009`
  ignore in `deny.toml` / `.cargo/audit.toml` without raising MSRV.
- Auth smoke covers PLAIN, SCRAM-SHA-256/512, and OAUTHBEARER over SASL_SSL;
  CI `test` jobs assert `openssl version` before `cargo test`.
- ADOPTION / CIVILIZATION record local civilization-check evidence
  (broker + SASL_SSL PLAIN + SCRAM-256/512 + OAUTHBEARER) while GitHub Actions
  remains queued.

### Fixed

- Mock TLS fixture reads cert/key PEMs via `File` + `Read` instead of
  `std::fs::read`, restoring `clippy::disallowed_methods` / publish-ready.
- `release.yml` only runs on final `vX.Y.Z` tags (not `v0.1.0-rc.*`), so git
  RC install pins do not queue crates.io publish jobs or burn Actions capacity.

## [0.1.0] - 2026-09-04

First crates.io release baseline (publish via `docs/RELEASE.md` / tag `v0.1.0`).

### Added

- Pure-Rust Kafka client: produce, fetch, classic groups, cooperative-sticky,
  KIP-848 consumer groups, KIP-932 share groups, transactions / EOS, and
  Kafka 3.x/4.x admin APIs.
- TLS via rustls (no OpenSSL); SASL PLAIN, SCRAM-SHA-256/512, OAUTHBEARER,
  OIDC client credentials.
- Compression: gzip, snappy, lz4 (zstd and Kerberos remain out of default
  features; see `docs/gaps.md` and `docs/zstd-spike.md`).
- Java-shaped builders and rustdoc naming matching Java client calls.
- Optional `tracing` feature (produce/fetch/poll/rejoin/txn spans).
- Examples: produce, consume, group, txn, eos, admin, tls, sasl, oauth, share,
  benches, interceptors, metrics (`FORMAT=prom`).
- Operator docs: guide, rdkafka migration, security, release policy,
  API stability, civilization plan.
- CI: mock tests (MSRV 1.85 + stable), clippy, audit, cargo-deny, `cargo package`,
  broker-smoke (Kafka 3.9.1 + 4.1.0), fuzz-smoke, latency-gate.
- libFuzzer targets under `fuzz/`; decode allocation DoS guards.
- TLS PEM parsing via `rustls-pki-types` (no archived `rustls-pemfile`).

### Fixed

- KIP-932 share groups on Kafka 4.1: join with a client Uuid member id, wait for
  a real heartbeat assignment (no zero topic id), decode null Assignment as
  INT8 `-1`, and use the group coordinator for membership. Share smoke needs
  `group.share.enable` and finalized `share.version=1`.

### Changed

- Lab A produce harness exits non-zero unless broker HW sum equals acked each run.
- Civilization/publish-ready gates verify a downstream crate can depend on the packed `.crate`.
- Release workflow accepts `workflow_dispatch` on an existing `v*` tag; rustdoc/`ci-docs` smoke is part of publish-ready.
- Fixed broken rustdoc `[Display]` intra-doc links (now `std::fmt::Display`).
- Cleared remaining unresolved rustdoc intra-doc links (module docs resolve in
  submodule scope; crate denies `rustdoc::broken_intra_doc_links`; `ci-docs`
  fails the gate if any reappear).
- CI: `dev/**` branch pushes run a single `branch-lite` job; full matrix on
  pull_request, `main`, and `workflow_dispatch` so scarce runners can finish.
- Lab A harness accepts `TOPIC=` as well as `KAFKA_TOPIC=`; STATUS notes a
  fresh unsigned latency-gate + HW==acked smoke sample (not a Suite HOLD lift).
- `scripts/check-installable.sh` probes crates.io for the Installable bar;
  ADOPTION notes Actions is stuck org-wide (`main` queued for hours).
- `ci-broker-smoke`: when Docker overlay fails but `$KAFKA_BOOTSTRAP` is already
  up, fall back to that broker automatically (same path as `SKIP_DOCKER=1`).
- Fuzz: Join/Sync/Heartbeat/OffsetCommit + ShareFetch decode targets; Lab A
  fetch integrity harness (`scripts/lab-a-fetch.sh`) requires consumed==seeded
  (unsigned; not a Suite HOLD lift).
- `examples/oauth` for OAUTHBEARER (unsecured JWT) and OIDC client-credentials;
  ADOPTION documents pinable `v0.1.0-rc.2` git install until crates.io lands.
- Broker smoke: Kafka CI matrix uses `apache/kafka:4.1.0`; Docker 4.x starts with
  share coordinator RF=1 and upgrades `share.version=1`; `REQUIRE_SHARE=1` fails
  the job if share cannot fetch records on 4.x. Civilization-check only counts
  broker smoke when the log contains `ci-broker-smoke: ok` (Docker soft-skips
  are not evidence).

### Security

- Reject array / tagged-field lengths that exceed remaining buffer bytes so
  malformed broker frames cannot force multi-gigabyte `Vec` allocations.

### Notes

- Not a drop-in for `rd_kafka_*` or rust-rdkafka types.
- Defaults that differ from Java: `auto.offset.reset=Earliest`,
  `allow.auto.create.topics=false`, shorter `delivery.timeout.ms` /
  `max.block.ms` (see README).
- Mock TLS fixtures use the `openssl` CLI (no `rcgen` / `time` in the graph).
