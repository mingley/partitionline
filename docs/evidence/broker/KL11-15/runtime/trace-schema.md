Actual three-/five-replica snapshot histories use schema_version2 and profile
`fixed-membership-full-prefix-snapshot`. They preserve the KL11-73 event envelope:
ordinal, monotonic now_ms, kind, executing node_id, actual args/result and after
states for every node. The group pins bounded cluster/topic strings, partition
and fixed sorted voters. The final source_sha must resolve to the pushed source
used for the compiled tests, never an uncommitted tree.

Every after-state records receiver-local term/vote/leader, role, readiness,
poison state, active replication term, durable tail and commit, exact committed
record term/index/kind/payload, both durable operation counts, and the selected
image descriptor/base. An image descriptor contains generation hex, inclusive
normalized base position, records, exact payload/encoded byte counts and CRC32C.
No emitted verdict substitutes for independent parsing of captured bytes.

Snapshot-specific events are `checkpoint`, `prepare_snapshot`, `begin_snapshot`,
`snapshot_chunk`, `receive_snapshot_chunk`, `partial_snapshot_chunk`,
`finish_incomplete_snapshot`, `abort_snapshot`, `finish_snapshot`,
`drop_snapshot_ack` and `ack_snapshot`. Offers include leader, peer, sequence,
current term, leader_commit and the whole descriptor. Chunk outputs/inputs retain
exact offset, bytes_hex and emitted done status. A partial delivery names the
ordinal of the emitted source chunk and declares its prefix slicing. A forged
receipt explicitly names its emitted response origin and changed checksum field;
it is a rejected input and cannot contribute to majority evidence. Actual lost
receipts retain their bytes and fail after the source timeout releases correlation.
Existing campaign/vote/activation/proposal/append/ACK/quorum-expiry events remain
actual calls of the compiled Node, with an explicit current-term barrier.

`journal-receipts.jsonl` and trace.final_journals name each immutable checkpoint's
metadata.wal, election.wal and images directory, phase/event ordinal, confirmed
operation counts and selected descriptor. All complete generations are copied,
including inactive ones. Parent reopening happens after an actual child process
exits without owner Drop. Raw journals/images before and after recovery are the
independent proof inputs; checksums/manifests are added when captures freeze.
The child writes its final logical clock to `restart-now-ms.txt` before exit;
parent owners reopen at that parsed value. This preserves one monotonic test
clock across the process boundary. Earlier development captures reset the parent
clock to zero and remain retained as rejected trace inputs; their recorded times
are never rewritten.

The separate `install-cut-{receipt,summary}` artifacts are narrower owner tests:
synthetic typed configured-leader inputs prepare a genuine durable prefix and
image, then a cfg(test)-only generation-targeted hook terminates the actual
process after Install-WAL-sync/pre-summary or after summary/pre-reply. These are
not election-majority proofs. The before-/after-reopen raw trees and cut.json bind
prior tail/commit and expected unchanged receiver term/vote. They prove recovery
and ordering at an owner boundary; actual quorum histories above prove causal
majority and committed-prefix continuity.

`invalid-install-receipts` retains ten complete outer-Journal-checksummed field
mutations, their changed offset, actual error and all referenced images. The
positive original recovers; each mutated one rejects. `image-published-wal-failed`
uses an actual closed-file known torn-tail append to make the owner's post-image
WAL append fail: it poisons, reopens under Journal recovery, and keeps the old
selected image plus the committed later WAL suffix while the new image is inert.
These deliberate altered inputs are separate from unchanged emitted exchanges.
