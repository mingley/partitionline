# Muted Fetch bootstrap reconnection

A closed bootstrap socket could be skipped while a Fetch was waiting for a broker quota. If the background watcher marked it closed before the quota watcher scanned the leader, the quota watcher could wait without reconnecting. The fix recognizes the already-closed socket, releases it and opens a replacement. It resets the Fetch session and applies reconnect backoff after failed replacement attempts.

Two deterministic main-tree tests passed: one starts the watcher with the bootstrap already marked closed and checks replacement negotiation, session reset and absence of an extra request; the other verifies repeated refused dials follow the configured backoff. Five existing tests also passed individually: quota-watch cancellation, a queued byte during quota wait, a bootstrap peer closing during quota wait, a later leader closing during quota wait, and closure during session retry. Strict all-features library Clippy passed. Exact commands, outcomes, source hashes and logs are in the compressed receipts.

Before the fix, the quota-close integration case failed in a serial bootstrap suite and again unchanged on its own: it did not observe a replacement connection within its 400 ms limit. These failures were on a different isolated candidate, not the final main-tree source; the two versions shared the condition that skipped an already-closed bootstrap. The retained logs do not expose the ordering between the background and quota watchers. The deterministic closed-first test validates that specific ordering. The old suite also had an unrelated Admin ListOffsets timing failure; the full failure is retained in the before log.

## Source and limits

The integrated runs used the main working tree at HEAD `9a3e65651f52949ede84d3720c3b93e5f170f06f`, with the quota patch uncommitted. The main `consumer.rs` snapshot and patch are included. All eight integrated receipts have the same 7,718 input hashes and report stable inputs. Their shared input snapshot is included; it covers the runner's listed repository directories and files, not every external tool, operating-system state or dependency. The before receipts pin their own different source inputs and include their pre-fix `consumer.rs` snapshot.

The library test executable was freshly built from the main checkout after refreshing file timestamps and was checked for both new test names. Its SHA-256 is `7f951a05e18303a9665b94d7fab2c4fe07435b8645f97031b44e567eb6d683b5`; a retained copy is referenced in the test receipt but is not included here. The bootstrap test executable is recorded by hash in its receipts and is not retained here. The build used stable Rust on x86_64. The local runner and compiler versions are recorded in the receipts.

These focused checks do not establish full client correctness, production readiness or a performance ranking. The broader fresh-connection negotiation work was not part of this main-tree patch. Verify the stored and reconstructed archive files with:

```sh
python3 benchmarks/runtime/verify-evidence-archive.py docs/evidence/client/muted-fetch-bootstrap-20261028
```
