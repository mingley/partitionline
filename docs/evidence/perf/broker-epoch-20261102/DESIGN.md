# Candidate design: bounded replica epoch publication

Status: design only. No production code, native test or benchmark was run.
The local replica is caller-driven and is not the broker's production data
replication implementation. The current full-file rewrite happens only when
`replicate_from` raises the persisted epoch. It is not an every-append cost.

## Existing guarantees and constraints

The legacy 24-byte header stores the base offset, an encoded epoch and CRC.
Recovery accepts legacy event-only logs and takes the maximum of the header
and every replayed record epoch, including records later logically truncated.
For `Log`, the base offset is always zero. Epoch publication currently
copies the entire journal to a private temporary file, validates every copied
header and payload, rebuilds and compares its bounded index, syncs the new
file, atomically renames it, then syncs the parent directory. An ambiguous
rename or post-rename directory sync poisons the live handle.

This preserves a configured file limit that may leave room for exactly one
entry; epoch-only updates consume no journal entries or index slots. Legacy
general readers reject nonzero replica headers. Older replica implementations
must not silently read a new file and ignore a newer external epoch.

External mutation and concurrent processes are outside the documented
exclusive-ownership contract. Nevertheless, the current rewrite detects
several same-size changes to source headers, payload CRCs and index fields;
one existing test intentionally violates exclusive ownership to verify this.
Any change to that observable behavior needs an explicit decision and tests.

## Format sketch

- Keep the journal header exactly 24 bytes. Define a *replica-only* v2 magic,
  12 bytes of randomly generated journal ID and CRC32C. The current replica
  user always opens base offset zero; the v2 decoder must reject any other
  requested base offset. Leave generic journal readers unchanged so both old
  generic readers and old replica readers reject the new magic.
- Use a small sidecar whose name contains the full journal ID. Its fixed,
  checksummed record contains its format version, the same ID, a nonnegative
  epoch and, if needed for diagnostics, a generation number. A sidecar with
  the wrong ID, malformed CRC/length/version, missing required fields or an
  absent file makes a v2 journal fail closed. Never fall back to a legacy
  header or an empty fence. Random-ID collision resistance reduces accidental
  cross-pairing; CRC is not authentication, and rollback of a complete old
  backup is not detected by the existing format either.
- Keep `Limits::max_file_bytes` and `max_index_entries` semantics unchanged.
  The sidecar is additional bounded filesystem storage, accounted and
  documented separately. Preserve journal and sidecar file permissions before
  payload writes and after writes that clear set-ID bits; fsync files and parent
  directories. Reject symlinks, nonregular reserved paths and mismatched IDs.
- New software can open old journal formats. After migration, old binaries
  refuse the v2 magic. A completed v2 backup needs a consistent journal and
  sidecar pair; copying only the journal must fail closed, not lose a fence.
  The migration/backup operational change is real and is not full bidirectional
  compatibility. Hard-link aliases and cross-process ownership remain limited
  by the existing process-local guard contract.

## Migration sketch

1. Replay the old journal fully to recover `accepted_epoch` before selecting
   a new target. Require the requested target at least that recovered maximum.
   Generate an ID with `getrandom`; RNG failures fail closed. Reserve all
   journal, sidecar, migration marker and temporary paths/inodes with the
   process-local ownership guards. Fail on unknown or mismatched preexisting
   reserved files rather than overwriting them.
2. Publish and sync a small migration marker, recording the old source
   identity/header and the candidate ID. Publish and sync the ID-bound
   sidecar at the target epoch. The marker identifies any sidecar as
   nonauthoritative while the old journal remains installed.
3. Perform one old-style whole-journal replacement: copy the unchanged
   entries under the v2 header, validate all contents and the rebuilt index,
   sync the temporary journal, rename it and sync the parent. Transfer the
   journal inode guard without a gap.
4. Remove the migration marker and sync the directory. If any operation after
   an attempted journal rename or after an uncertain ownership transfer
   fails, poison the live handle. Recovery resolves the marker before serving
   reads or accepting writes.

One-time migration is O(journal bytes); it keeps current file/index budgets.
It must not run blindly on each epoch. The marker and cleanup design needs
further review for inode reuse, hard links, externally restored journals and
platforms without a stable file identifier. Do not automatically delete an
unknown file merely because its name resembles an orphan.

## Crash and error matrix

| Interruption | Required recovery behavior |
| --- | --- |
| Before marker publication | Legacy journal remains authoritative; remove only a recognized temporary file. |
| Marker durable, sidecar absent or temporary | Legacy journal remains authoritative; recognize and clean the interrupted migration or fail closed if identity cannot be proven. |
| Sidecar durable, old journal still authoritative | The prepared sidecar is not a successful epoch publication. Validate the marker and source identity; clean it or fail closed. Reopening must not silently advance the old journal's epoch. |
| Journal rename attempted, directory sync not confirmed | Either old journal plus identified uncommitted sidecar, or v2 journal plus matching sidecar may survive. Validate whichever state exists; all other combinations fail closed. The live handle is poisoned. |
| V2 journal and sidecar durable, marker still exists | Validate the pair; clean the recognized marker and sync its directory before use. |
| Later sidecar temp write/sync fails before rename | Original sidecar remains authoritative; do not advance the in-memory fence. |
| Sidecar rename attempted or parent sync fails | Poison the live handle. On reopen, accept only a complete matching old or new sidecar. No caller observes success until directory sync completes. |
| V2 sidecar absent, malformed, wrong ID or unsupported | Fail closed. Do not reconstruct an epoch from records as a fallback: the last accepted epoch may have no records. |
| Backup/copy restores only one member or mismatches IDs | Fail closed and require operator repair. A valid older pair remains a rollback risk, as an older complete journal already is today. |

The atomic-rename reasoning relies on filesystem and hardware synchronization
semantics, just as the current implementation does. A process-exit test alone
does not prove power-loss behavior.

## Per-epoch operation

Validate the live journal/header and current sidecar identity, ensure the new
epoch is monotonic against the full in-memory accepted epoch, and prepare a
new small sidecar beside the old one. Set private permissions before any
write, write the complete record, restore mode bits if necessary, sync it,
rename over the old sidecar, transfer its inode reservation, and sync the
directory. Only then update the in-memory fence and allow destructive
replication operations. Maintain the established poison-on-ambiguity rule.
Do not charge metadata to user record/index capacity or skip the epoch check
when no records are copied. Appends continue to record their epochs in journal
events; replay takes the maximum of events and the sidecar.

Writes and validation are O(1) metadata bytes per epoch, plus durable sync
latency. No throughput or recovery-time factor is asserted. The design cannot
also promise the current full-file source-integrity check at every epoch in
O(1): detecting arbitrary same-length mutation requires reading the relevant
bytes or relying on an authenticated immutable storage layer. Length, inode,
timestamps or cached hashes alone do not prove unchanged contents. The safe
choices are to retain the full scan, explicitly narrow the source-mutation
contract under exclusive ownership, or add a separately qualified integrity
mechanism. The existing adversarial-mutation test must not be quietly dropped.

## Required qualification

- Old empty, one-entry/full-budget and nonempty logs migrate; events and
  offsets remain byte-identical; prior reader binaries reject v2.
- Reopen after a fence with no new record, after logical truncation, after a
  later append at a higher epoch, and after every crash/error stage above.
- Inject partial writes, bad CRC/ID, stale or missing marker/sidecar, failed
  temp/rename/file sync/directory sync, symlink/FIFO/hard-link collisions,
  permission modes including set-ID, source changes, cleanup failure and RNG
  failure. Assert both fence monotonicity and poison/error classification.
- Compare file and sidecar bounds, record/index capacity and post-failure
  backup/restore. Cover process exit and separately document power-loss and
  filesystem guarantees. Retain all existing source-integrity controls.
- Measure bytes read/written, syscalls, latency distribution and peak disk on
  equal journal sizes and hardware after correctness; do not infer a Kafka
  throughput improvement from a metadata microbenchmark.
