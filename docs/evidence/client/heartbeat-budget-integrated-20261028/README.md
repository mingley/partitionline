# Integrated heartbeat metadata budget checks

On 2026-10-28, the main working tree's KIP-848 heartbeat decoder candidate
passed seven focused tests and strict all-features library Clippy on x86_64.
The decoder shares a finite metadata budget across heartbeat strings, topic
arrays and partition arrays. It checks minimum wire sizes before admitting
topic and partition counts.

The tests used a 2 MiB Rust thread stack. Five cover shared allocation limits,
segmented input, malformed counts, cancellation after a cooperative yield,
and a fully decoded 100,000-topic assignment. The remaining tests cover the
existing million-topic cooperative fairness case and KIP-848 heartbeat
scheduling. The fairness test can finish through its timer branch; it does not
prove the million-topic decode completed. Exact case names, commands, results
and elapsed times are in `test-results.json` and the compressed test logs.

`cargo +stable clippy --locked --all-features --lib -- -D warnings` passed.
This checks the library, not every test target. The test build used default
features; all-features applies to the Clippy run.

## Source and artifacts

The test ran from `/workspace/partitionline` at HEAD
`6e684b22832da6962facf5a8ecfff4f7e8f860c5` with the heartbeat change
uncommitted. `status-before.txt` and `tracked-source.patch.gz` record that
state. `inputs-before.sha256.gz` and `inputs-after.sha256.gz` contain matching
lists of 8,014 visible files selected by `rg --files --hidden`, excluding
docs, `.git` and target output. The run did not pin ignored files or external
dependencies; it is not a complete machine or source-tree identity. The
Unreleased changelog note was added after the checks.

The build used stable Rust 1.99.1 and the shared Cargo target:

```sh
RUSTUP_HOME=/workspace/work/rustup CARGO_HOME=/workspace/work/cargo \
PATH=/workspace/work/cargo/bin:$PATH \
CARGO_TARGET_DIR=/workspace/work/qualification-20261027/broker-check-target \
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 \
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
cargo +stable test --locked --no-run --lib --test full_surface --message-format=json-render-diagnostics
```

Cargo's JSON artifact records mark both test executables as freshly built from
the main-tree package. `executables.json` contains their hashes and retained
paths; the binaries remain in the work directory, not this archive. `run.py`
records the full sequence, including source checks, per-test deadlines and
the Clippy invocation. The tests ran the retained copies, not later files in
the shared target.

The metadata limit does not cover allocator bookkeeping or a temporarily
returned excess allocation. String copies, tagged-field scans, vector growth,
and destruction can still run synchronously. These checks do not establish a
global memory limit, production readiness, or a performance ranking.
