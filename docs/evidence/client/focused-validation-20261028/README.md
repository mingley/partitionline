# Focused client validation, 2026-10-28

These are focused checks on a shared x86_64 host using latest stable Rust. They are not a full test matrix, a production qualification, or a performance comparison.

The base commit was `cce150a918cfd754ade17e88f8d02a7b47f3869f`. The tested tree had uncommitted and untracked inputs, so every receipt correctly records `source_sha: null`; it must not be described as a clean commit. All 11 receipts report stable input hashes and the same input-file hash map. The dirty build/test inputs listed in the receipts are retained under `source/`, compressed with gzip, and their bytes were verified against the recorded hashes. Unchanged inputs come from the base commit; each receipt records its complete input hash scope. External tools and the environment are not exhaustively hashed. The archived source is evidence of this candidate, not a claim that the candidate has been merged.

Results, run serially with one test thread where applicable:

- Scratch name reuse: 1 passed. Session metadata accounting: 1 unit and 1 broker test passed.
- Network unit tests: 53 passed. TLS stalled-peer and cancellation tests: 2 passed. Bootstrap handshake: 83 passed.
- InitProducerId: 12 passed, 1 ignored. Network deadlines: 9 passed. ApiVersions allocation: 2 passed.
- Strict all-feature library Clippy passed.
- The legacy producer/consumer/group startup deadline test failed. Group startup returned broker error code 30 (`JoinGroup`) at about 57 ms; the original 60 ms request deadline and 90 ms elapsed assertion were not increased. The full failure is retained.

The preceding default matrix had additional failures and was not rerun in full for this candidate. The InitProducerId ignored case remains unqualified. ReadCommitted error rollback and public-fetch cancellation have separately identified correctness gaps, which this focused sequence does not resolve. An isolated reconnect candidate remains unintegrated and had broad-suite failures. No full all-target or production readiness claim follows from these checks.

The raw logs and unmodified JSON receipts are under `runs/`. Receipts name any locally retained executable snapshots and their hashes; those binaries are not included in this archive. The runner was invoked with a 300-second process timeout per case and captured cgroup memory events and free disk. No OOM kill was recorded in the receipts. Hashes and decompressed sizes are recorded in `archive-inventory.json`.

Verify the archive from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/client/focused-validation-20261028
```
