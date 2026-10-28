# Integrated broker candidate checks

Two targeted checks passed on the uncommitted integrated candidate based on
commit `263541e7d4ed8e578f039427de9b91f083e39c9d`, on Linux x86_64 with latest
stable Rust. The commands and raw outputs are retained in `runs/`.

- The operator process test passed: it checked its seven advertised API ranges,
  Metadata address, a fixture-based acknowledged Produce and Fetch, data lock,
  SIGTERM shutdown, identity mismatch and persisted data after restart.
- Ten quota unit tests passed, covering independent bucket refill, delayed work
  debt, bounded identity state, rate changes and overflow handling.

These are targeted checks. They do not qualify the complete broker, the Kafka
wire quota feature, security, replicated durability or performance. Independent
live client tests and both full broker test/lint matrices remain required.

Both receipts have `source_sha: null` and record the base commit separately:
the tested sources were uncommitted. The runner hashed all Rust files under
`src/` and `partitionline-broker/src/` before each run and verified they stayed
unchanged through that run. Those per-file hashes are in the receipts. The
selected broker-source copies under `source/` were checked against both receipts
when this archive was created. The runner did not pin `Cargo.toml`, `Cargo.lock`,
the operator integration test, fixture bytes, or a full workspace snapshot;
their copies are post-run snapshots and cannot prove those inputs were unchanged
at execution time. Retest from a clean committed tree for an exact-source gate.

Both executed test binaries were copied after the runs and matched the exact
SHA-256 values in their respective receipts. The separate operator program
copied alongside them is a post-run snapshot: the runner did not record its
hash at execution time, so it is not represented as an identified executed
binary. The published RF1 worktree candidate and its own test are documented
separately at `../rf1-operator-20261028/`.

Verify stored and reconstructed files from the repository root:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/integrated-candidate-20261028
```
