# Clean client default validation, 2026-10-28

These are local x86_64 checks on a shared host. They document the commands and cases below; they do not qualify production readiness, complete Kafka compatibility, or a fastest-performance claim. The matrix used default features only and does not qualify all-feature behavior.

All cases used clean commit `53b5af47fd18898a68364d9b88edc181c225bfe4`, with stable before/after inputs and HEAD. The runner hashes the listed source and test inputs before and after each case; it cannot detect transient changes reverted between captures. It does not exhaustively capture the kernel, external tools, credentials or environment. Test executable hashes are recorded, but no executable snapshots are included: the filesystem rejected `cp --reflink=always`, so the runner recorded hash-only retention. Later Cargo builds may replace the target-path binaries.

- The standalone `verifiable_producer` and `verifiable_consumer` examples built successfully.
- `cargo +stable test --locked --all-targets --no-fail-fast` exited 101: **3,354 passed, 76 failed, 37 ignored** across 81 reported target results. Eight targets failed: `--lib`, `buffer_ownership`, `consumer_fetch_semantics`, `full_surface`, `legacy_discovery_v0`, `mock_produce`, `offset_client`, and `producer_retained_backing`. Failure lines include scheduling and deadline tests; the run alone does not establish each failure's cause.
- [test-index.md](test-index.md) lists every reported failure and ignored test by target. The raw log contains failure output and full target results. Ignored cases were not exercised; other tests may also depend on external services or environment variables.

The cgroup memory limit was 16 GiB and CPU quota was four CPUs. The run used two Cargo build jobs and Cargo's default test threading. Each command had a 2,400-second timeout and a 2 GiB emergency disk floor; new cases required 2.1 GiB free. Neither command was stopped for timeout or disk, and no OOM kill was recorded. Minimum recorded free space during the matrix was 3,184,082,944 bytes. Samples, commands, Rust version, input hashes and limitations are in the raw receipts.

The logs and unmodified JSON receipts are under `runs/`; the runner and client sequence driver are under `runner/`. `archive-inventory.json` records stored and reconstructed hashes and sizes. Verify from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/client/clean-default-validation-20261028
```
