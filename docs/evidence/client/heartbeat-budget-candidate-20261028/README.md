# Consumer group heartbeat metadata budget candidate

This receipt covers a scratch candidate, not a change applied to the main
worktree. It adds a per-message metadata budget to the KIP-848 heartbeat
request and the synchronous and cooperative response decoders. The budget
covers strings, topic arrays, and partition arrays together. The candidate
also checks topic and partition counts against their minimum wire sizes before
reserving vectors. The isolated change is in
`parser-budget-prototype-cgheartbeat.patch.gz`.

Seven exact tests passed on the saved binaries with `RUST_MIN_STACK=2097152`:

- Five candidate tests cover shared nested allocation limits, request string
  accounting, segmented input, malformed topic and partition counts,
  cancellation after a cooperative yield, and a complete valid 100,000-topic
  assignment decoded by both response paths.
- The existing million-topic cooperative fairness test passed. It can finish
  through its timer branch, so it does not demonstrate a complete decode.
- The KIP-848 broker heartbeat scheduling integration test passed.

`test-results.json`, `run-tests.py`, and the seven compressed test logs record exact
commands, durations, and results. The all-features library Clippy run **failed**
on two `records.rs` lints and an unfulfilled `fetch.rs` lint expectation in
the scratch snapshot. See `clippy-all-features-lib.log`; no Clippy pass is
claimed. The candidate still needs validation on the final integrated source.

## Source and commands

The scratch worktree used commit
`22b50ae27632fdd7b66cff36bab1f9b6a3cceda8` with additional uncommitted source
changes. `parser-budget-prototype-build-source.patch.gz` records its complete
`src` diff against that commit. `inputs-at-build.sha256.gz` contains a hash
list of all 8,007 visible scratch files selected by `rg --files --hidden`,
excluding `.git` and the target directory. The same paths and hashes were
recorded after the tests and after Clippy; `input-comparison.log` contains the
comparisons. This does not pin ignored files, dependencies, or the entire main
working tree.

The build used stable Rust 1.99.1 on x86_64, with a shared Cargo target:

```sh
RUSTUP_HOME=/workspace/work/rustup CARGO_HOME=/workspace/work/cargo \
PATH=/workspace/work/cargo/bin:$PATH \
CARGO_TARGET_DIR=/workspace/work/qualification-20261027/broker-check-target \
CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 \
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
cargo +stable test --locked --no-run --lib --test full_surface
```

The exact produced binaries were copied before execution; their original and
saved hashes match in `executable.sha256`. They remain in the work directory,
not this archive. `run-tests.py` runs the saved binaries individually, with a
120-second deadline for each case. The failed Clippy command used the same
environment and target with:

```sh
cargo +stable clippy --locked --all-features --lib -- -D warnings
```

The candidate's 128 MiB default is an intentional compatibility limit for
large valid messages; its explicit-limit variants permit a different bound.
The limit excludes allocator bookkeeping and temporarily returned excess
capacity. Individual string copies, tagged-field scans, vector growth and
destruction remain synchronous. This receipt does not establish a strict
per-poll latency bound, a global cross-connection memory limit, production
readiness, or a performance ranking.
