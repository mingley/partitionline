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

GitHub Actions: pushes to `dev/**` run a single `branch-lite` job (fmt, clippy,
lib tests, docs). The full matrix (MSRV, broker smoke, fuzz, deny, package, …)
runs on pull requests, `main`, and `workflow_dispatch`. Prefer opening a PR (or
dispatching the workflow) when you need the full gate.

Real-broker smoke (Docker required; skipped locally if Docker is missing unless `CI=true`):

```
bash scripts/ci-broker-smoke.sh
# optional: KAFKA_IMAGE=apache/kafka:4.0.0 bash scripts/ci-broker-smoke.sh
# TLS + SCRAM (isolated ports; needs local Kafka/Java/openssl):
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

MSRV is 1.85. `rcgen` pulls `time`; keep `time` at **0.3.41** in `Cargo.lock` (`0.3.55` needs rustc 1.88).

Do not claim Suite HOLD / signed bench wins without the process in `docs/STATUS.md` and `docs/benchmark.md`.

When mapping Java helpers, name the Java API and spoken version range in the
commit message (existing convention).
