# Single-node operator candidate

This candidate adds an ordinary plaintext RF1 listener to the broker operator.
It binds an explicitly configured address, advertises the configured endpoint,
reopens a catalog and partition journals, holds a data-directory lock, checks a
persisted cluster/node identity, and handles SIGINT and SIGTERM by awaiting both
listener and router shutdown. Its existing router advertises only Produce 3–13,
Fetch 4–6, ListOffsets 1–3, Metadata 0–13, ApiVersions 0–4, CreateTopics 2–4,
and DeleteTopics 1–6. This local profile has no data replication, producer
idempotence, transactions, consumer-group coordination, TLS or SASL.

The process test passed on Linux x86_64 with latest stable Rust 1.99.1. It
checked the exact API-version list and Metadata endpoint, a Kafka request
fixture through CreateTopics, an acknowledged Produce and Fetch, single-owner
locking, a clean SIGTERM and joined shutdown, wrong-identity rejection, and
byte-identical Fetch after restarting on the same data directory. It compares
the protected batch bytes and high watermark. It is a bounded local process
test using existing independent fixture bytes; it is not a fresh live Java or
librdkafka peer run, a fault campaign, a complete broker qualification, or a
performance comparison.

The candidate was built in sparse worktree `/workspace/work/partitionline-rf1`
at base commit `64da3d2db`, with uncommitted changes. The receipt hashes the
broker source, manifests, fixtures and actual copied executables used. The main
worktree has additional quota and client edits; this test does not establish a
pass for that integrated tree. Re-run the full suites and independent clients
against a committed candidate.

`rf1-check-01.log` and `rf1-check-02.log` failed because the initial sparse
checkout omitted files required by the parent Cargo manifest. `rf1-check-03.log`
recorded a module-path compilation failure. After correction,
`rf1-test-01.log` passed; `rf1-clippy-01.log` failed on two blocking-method
lints in the synchronous process test and one iterator lint. The scoped test
lint changes were followed by passing `rf1-clippy-02.log` and `rf1-test-02.log`.
The Cargo feature resolution added `errno` and `signal-hook-registry` for Tokio
signal handling; the complete lockfile is retained.

Commands, from the candidate worktree, used Rust 1.99.1 with offline cached
dependencies, four build jobs, incremental compilation disabled, and debug
information disabled for dev/test profiles:

```sh
cargo +stable check --offline --manifest-path partitionline-broker/Cargo.toml --bin operator
cargo +stable test --offline --locked --manifest-path partitionline-broker/Cargo.toml --test operator_server -- --nocapture
cargo +stable clippy --offline --locked --manifest-path partitionline-broker/Cargo.toml --bin operator --test operator_server -- -D warnings
```

Verify stored and reconstructed files from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/rf1-operator-20261028
```
