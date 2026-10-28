# Broker buffer reservation checks

Commit `923bc2ad8` fixes the total-capacity calculations for the metadata
response writer and the metadata and produce batch lists. Each requests the
required capacity relative to the vector's length, and leaves sufficient
capacity untouched. The tests exercise retained capacity, nonempty contents,
the response size limit, and a deterministic fallible allocation failure.

On Linux x86_64 using the stable toolchain, the final commit passed the two
metadata tests selected by `reused_`, one unrelated matching quorum test, and
the produce batch-list test. Strict Clippy passed for the broker library and
all test targets. The final runs used a sparse worktree at `923bc2ad8`, whose
parent is `af15516c8`; both changed source files matched the commit after the
runs and their commit versions are included here. The test executable, all
build inputs, dependencies, environment, and pre-run file hashes were not
captured, so this is not an independently reproducible whole-build receipt or
proof of unchanged inputs throughout execution. The stable binaries report
Rust and Cargo 1.99.1 at archive time.

`runs/reused-tests.log`, `runs/produce-test.log`, and
`runs/clippy-final-02.log` are raw command output; each command returned zero.
`runs/clippy-final.log` is raw output from an earlier Clippy attempt on the
same final commit that returned 101 because the sparse worktree lacked
`docs/evidence/broker/KL11-59/topic-name-cases.tsv`. The path was added to the
sparse checkout before the passing rerun. The earlier failed Clippy output on
base `352c2a315` is **reconstructed from tool output**, not a retained raw
execution log; `runs/clippy-on-original-reconstructed.txt` records its command,
output, and exit code. The later base included the fix for that unrelated lint.

The final commands below were run from the sparse checkout. The target directory
was shared, and raw logs were captured with `tee` and `pipefail`:

```sh
export PATH=/workspace/work/cargo/bin:$PATH
export RUSTUP_HOME=/workspace/work/rustup
export CARGO_HOME=/workspace/work/cargo
export CARGO_TARGET_DIR=/workspace/work/qualification-20261027/broker-check-target
export CARGO_BUILD_JOBS=2
cargo +stable test --locked --offline --manifest-path partitionline-broker/Cargo.toml --lib reused_ -- --nocapture
cargo +stable test --locked --offline --manifest-path partitionline-broker/Cargo.toml --lib a_multi_partition_sync_sizes_its_join_list_once
cargo +stable clippy --locked --offline --manifest-path partitionline-broker/Cargo.toml --lib --tests -- -D warnings
```

These checks verify the specific capacity and error behavior; they do not
qualify throughput or production readiness. The broader client-side patterns
identified during review require separate fixes and validation.

Verify stored files from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/buffer-reserve-20261028
```
