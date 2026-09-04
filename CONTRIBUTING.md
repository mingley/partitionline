# Contributing

This project is MIT OR Apache-2.0. Patches are under the same licenses unless you say otherwise.

Execution plan (what to build next, constraints, acceptance checks):
[docs/CIVILIZATION.md](docs/CIVILIZATION.md). Capability tracker: [docs/gaps.md](docs/gaps.md).
Release / semver: [docs/RELEASE.md](docs/RELEASE.md). Security: [docs/security.md](docs/security.md)
and [SECURITY.md](SECURITY.md).

```
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

GitHub Actions: `dev/**` tip pushes do **not** auto-queue CI (org runners were
starved by perpetual tip `branch-lite` re-queues). Tip gate locally:

```
bash scripts/ci-branch-lite.sh   # fmt, clippy, lib tests, docs
```

Full matrix (MSRV, broker smoke, fuzz, deny, package, …) runs on pull requests,
`main`, and `workflow_dispatch`. Open a PR (or dispatch the workflow) for the
full gate. If Actions stay queued, owner: `bash scripts/owner-cancel-stuck-runs.sh`.

Real-broker smoke (Docker required; skipped locally if Docker is missing unless `CI=true`):

```
bash scripts/ci-broker-smoke.sh
# optional: KAFKA_IMAGE=apache/kafka:4.0.0 bash scripts/ci-broker-smoke.sh
# TLS + PLAIN/SCRAM/OAUTHBEARER (isolated ports; needs local Kafka/Java/openssl):
bash scripts/ci-auth-smoke.sh
```

Optional feature compile:

```
cargo test --features tracing
```

libFuzzer smoke (nightly + cargo-fuzz):

```
bash scripts/ci-fuzz-smoke.sh
```

Supply-chain (`deny.toml`):

```
bash scripts/ci-deny.sh
```

Civilization bar self-check (package + deny + docs gates; broker optional):

```
bash scripts/ci-civilization-check.sh
```

Lab A integrity smoke (HW == acked and consumed == seeded + unsigned latency;
needs a broker — native Kafka is fine):

```
bash scripts/ci-integrity-smoke.sh
# or: REQUIRE_INTEGRITY=1 bash scripts/ci-integrity-smoke.sh
```

Before a crates.io cut (owner token required for the real publish):

```
bash scripts/ci-publish-ready.sh
```

When Docker overlay is unavailable (common in nested VMs), use a native broker
(defaults to Kafka 4.1 with `group.share.enable` and `share.version=1` so KIP-932
share smoke can run):

```
bash scripts/ci-native-kafka.sh start
SKIP_DOCKER=1 bash scripts/ci-broker-smoke.sh
bash scripts/ci-native-kafka.sh stop
```

Please do not add librdkafka, or C compression libraries, as default dependencies. This crate forbids `unsafe`.

MSRV is 1.85. Mock TLS fixtures use the `openssl` CLI (not `rcgen`) so the
dev graph stays free of `time` / RUSTSEC-2026-0009 without raising MSRV.

Do not claim Suite HOLD / signed bench wins without the process in `docs/STATUS.md` and `docs/benchmark.md`.

When mapping Java helpers, name the Java API and spoken version range in the
commit message (existing convention).
