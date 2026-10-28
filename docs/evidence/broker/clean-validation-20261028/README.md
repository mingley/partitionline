# Clean broker validation, 2026-10-28

These are local x86_64 checks on a shared host, using stable Rust 1.99.1. They cover the commands and test cases described here; they do not qualify production readiness, complete Kafka compatibility or a fastest-performance claim.

All recorded cases used clean commit `de38cd3df49daa73474156643de7fae294882cdb`. Each receipt records a clean before/after status, matching input hashes, and the same source commit. The runner hashes the listed source and test inputs before and after each case; it cannot detect a temporary change that was reverted between captures. It does not exhaustively capture the kernel, external tools, credentials or environment. Test executable hashes are recorded, but no executable snapshot is included: the filesystem rejected `cp --reflink=always`, so the runner recorded hash-only retention rather than claiming immutable copies. Later Cargo builds may replace the target-path binaries.

The original `broker-clean-matrix-01` sequence stopped before doctests because free disk fell below its configured 3 GiB case-start floor. Its summary remains **incomplete**. The observed results before that stop were:

- Broker formatting passed.
- Default-feature all-target tests: **687 passed, 0 failed, 0 reported ignored** across 36 target results; 10 target results contained zero tests.
- All-feature all-target tests: **830 passed, 1 failed, 0 reported ignored** across 36 target results. `oidc_sessions::revocation_cancels_active_handler_and_waiting_handler_admission` failed at `tests/oidc_sessions.rs:723`: expected handler-call count 1, observed 2. The cause is not established by this run.
- Strict all-feature all-target Clippy and all-feature documentation build passed.

After a separate Go build-cache cleanup, the omitted default and all-feature doctest commands were run as `broker-clean-doctests-01` on the same clean commit. Both commands succeeded and each discovered **zero doctests**. This follow-up does not change the original sequence's incomplete status or its all-feature failure. The cache-cleanup command, log and receipt are included separately.

The raw logs say zero tests were ignored, but this does not mean every external scenario ran. In particular, the `serve_live_probe` tests in broker Fetch, Metadata, Produce and Retention return early when their respective `PARTITIONLINE_*_LIVE_PORT` variables are absent; the `runtime_child_process` helper similarly depends on `PL_RUNTIME_CHILD_ROOT`. The runner did not record those variables, so these reported `ok` results do not establish that the corresponding external live-peer paths ran. Default features also leave feature-gated targets with zero discovered tests. Other independent peer, crash, soak and multi-node qualification remains separate.

The cgroup limit was 16 GiB and the CPU quota was four CPUs. The cases used two Cargo build jobs and Cargo's default test threading. Each command had a 2,400-second limit and a 2 GiB emergency disk floor; a new case required at least 3 GiB free. No case was terminated for timeout or emergency disk floor and no recorded OOM kill increased. Samples and exact command lines are in the raw receipts. Initial and final hashes establish the recorded tree, subject to the limitations above.

Raw logs and unmodified JSON receipts are under `runs/`; the exact local runner and separate doctest driver are under `runner/`. `archive-inventory.json` records stored and reconstructed hashes and sizes. Verify from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/clean-validation-20261028
```
