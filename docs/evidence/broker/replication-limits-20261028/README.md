# Replica log recovery limits candidate

This candidate corrects the encoded record header allowance from 21 to
25 bytes, checks follower payload and recovery fetch limits before truncating a
divergent suffix, and admits the truncate plus first replacement together against
journal index and file limits. It reserves index capacity before truncation and
reports already durable partial copying as progress. Recovery uses the configured
index limit for each fetch. A source log with an ambiguous write cannot be used
for replication until reopened.

The two operations are not atomic: a process exit or storage failure after a
successful truncate can leave the follower at the shortened prefix. A child
process test exercises reopen and retry at that point. It does not simulate power
loss or prove filesystem or hardware durability. The log has no committed
watermark guard and its leader epoch is derived from retained records; these
issues, network replication, broker fencing and full server integration remain
open. The accompanying [design](integration-design.md) sets out those gaps.

On stable Rust 1.99.1, x86_64, 26 selected replica tests passed under default
features and 26 under all features. Strict all-feature library Clippy passed.
Tests cover unequal payload limits, request and journal budget boundaries,
partial progress, one-entry recovery, pre-truncate index-reservation failure,
legacy journal recovery with updated limits, and process exit after truncation.
Tests used one thread. The records and commands are in the compressed `v3b`
receipt and logs. The current operator does not use this local log.

The test snapshot was copied from clean commit
`63aa045be5ceb85b6c2ff7ba5a9b30c26dc9b67b` and then modified by the included
v3 patch. The snapshot contains 6,058 selected files; their root/copy hashes
matched, and the patched snapshot's before/after test hashes matched. The
selected scope is listed in the receipt and excludes documentation, ignored
files, external dependencies, operating-system state, and toolchain binaries.
It was built with the shared Cargo target and forced source timestamp refresh;
Cargo reported freshly built test binaries. Their hashes and retained local
paths are in the receipt; the executables are not included in this archive.
This receipt is evidence for the isolated candidate, not the integrated tree.

The first `v3` build failed before compilation because the isolated snapshot
omitted workspace examples required by the root manifest. Its raw log and
receipt remain included. The `v3b` snapshot added 32 separately hash-verified
example files before the successful run; it did not erase the failure. The
failed receipt records its runner hash, but that earlier runner source was not
retained. The included runner is the v3b version. The runner uses a 300-second
process bound and a 2 GiB disk floor; no cgroup OOM
kill was observed in the successful receipts.

The corrected maximum-entry requirement can reject a configuration that an
older version accepted for smaller records. Raise the journal entry limit to
at least `max_payload_bytes + 25` before opening such a log. The test checks
that an old-format journal reopens with the migrated limit. Existing event
bytes are unchanged; the unsafe epoch-fence draft is not part of this patch.

The integrated main working tree then passed the same 26 selected tests under
both feature sets and strict all-feature library Clippy. Its base was commit
`63aa045be5ceb85b6c2ff7ba5a9b30c26dc9b67b`, with the v3 source changes and
untracked evidence directories present. All three `run-v2` receipts report
stable scoped inputs and the same base. The source hashes are listed in
`integrated-summary.json`; both fresh test binaries were copied and hash
verified locally, but are not included here. The build and test inputs were
hashed; external dependencies and tools are not exhaustively captured.
`run-integrated.py` records the extra freshness, copy and disk-floor checks.

These focused checks do not establish full broker behavior or tests, production
readiness, or a comparative performance result. Verify archived bytes from the
repository root with:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/broker/replication-limits-20261028
```
