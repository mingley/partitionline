# Transaction journal retention and checkpoint design

## Current contract and constraints

The coordinator journal can represent its current producer/transaction state in
a checkpoint. The transactional-offset and marker logs have a different
contract: callers may request committed history from arbitrary offsets, and
marker retries return their original offset indefinitely. A finite disk limit
cannot support infinite history and those guarantees without a retention API or
an explicit refusal to admit further writes. Replacing either event log with
only the latest maps would lose observable rows and alter the stable boundary.

The coordinator currently exposes component state, not complete Kafka
transaction orchestration. In particular, initializing an ongoing transaction
clears its partition set; it does not durably abort the prior epoch or send
markers to all participants. Checkpointing the coordinator does not fix that.

## Retention contract

Introduce monotonically increasing absolute sequence numbers, a persisted
`log_start`, and an explicit release operation. Release requires a caller's
durable acknowledgement and a requested cut no later than the stable
boundary. Reads before `log_start` must return a distinct out-of-range error;
reads at or above it retain ordering and offset semantics. Unresolved
transactions pin the stable boundary. If no safe cut is available, enforce
bounded backpressure rather than silently deleting data. Defining timeout or
abort requires a durable coordinator decision and marker propagation; absence
of a marker is not evidence of abort.

For each producer/epoch at the cut, retain the last event type. A trailing
marker must keep its original absolute offset and decision for idempotent retry;
a trailing data event means no marker currently closes that epoch even if an
older marker exists. Replay the retained suffix over this boundary table,
checking absolute offsets and same-key marker transitions. Keep the highest
producer epoch and the data needed for fencing until an explicit, safe expiry
rule exists. MarkerLog currently has no producer fencing or coordinator link;
those need an authoritative integration, not a local inference.

For transactional offsets, retain unresolved rows and all later rows in global
order, because the earliest unresolved data or offset blocks visibility of
later committed rows. Preserve the committed group-generation floors, counts
for every open generation, per-epoch staged offsets, trailing marker decisions,
and per-producer highest epoch. An abort releases only its epoch's open counts.
A new epoch while an older one remains open needs an explicit protocol choice:
reject the bump until closure, or durably abort the old epoch and propagate its
markers. Existing old open epochs cannot be silently discarded during migration.

Reserve enough event/index/file capacity for the closing marker of each admitted
open epoch. A full journal must never prevent closing a transaction whose first
row was already acknowledged. Reservations must survive recovery and release
only after the marker is durable. Existing journals already at capacity need a
repair or migration path; a new reservation check alone is insufficient.

## Multi-entry snapshot publication

A checkpoint larger than one configured entry requires a new generation file or
manifest with version, sequence boundary, chunk count, exact byte lengths,
checksums, and complete logical state. Bound every chunk, total bytes, decoded
objects, and retained suffix; do not allocate from attacker-controlled counts
before checking the bytes available and configured budgets. Chunk ordering and
identity must be checked and duplicates, gaps, bad checksums, and invalid state
rejected. A completed snapshot can be large, so use streaming or explicitly
budgeted buffers rather than an infallible complete map clone.

Write new generation chunks to an exclusive temporary location; synchronize
all data before atomically publishing the generation/manifest, then synchronize
its parent directory. Recovery selects exactly one complete generation and
replays its WAL suffix. Before publication, the old generation remains
recoverable; after an ambiguous rename or sync failure, poison all reads and
writes until reopen. Do not unlink the old generation until publication is
confirmed and retention/migration requirements permit it. Define same-filesystem,
exclusive ownership, crash-orphan cleanup, disk-space and directory-sync
semantics explicitly. Process exits are useful tests but do not prove power-loss
durability on a filesystem or device.

Existing event-only journals must remain readable, even when current state
exceeds the maximum single-entry size. Migrate only after the new generation is
complete and durable; older binaries cannot read a new-format journal unless a
separate backward-compatible export is provided. Neither lower per-entry limits
nor lack of disk space should delete the old history. Until multi-entry
publication exists, retain the event-only compatibility path and its finite
history limitation.

## Validation

Use differential histories against an uncompacted model: interleaved producer
epochs, same-epoch successive transactions, committed/aborted rows on both sides
of a cut, an unresolved row blocking a later commit, static and transaction
generation changes, stale epochs, original marker offsets, duplicate/opposite
markers, all retained read offsets, and rejection before `log_start`. Reopen
after each operation and after repeated cuts. Test history-size and identity
ledger bounds, reservations at capacity, oversized legacy state, interrupted
and malformed chunks, wrong counts/checksums, suffix gaps, overflow and
idempotent retry. Use child-process exits and injected failures at data sync,
manifest publication and directory sync, preserving old/new complete-state
results. Run controlled filesystem and power-loss testing separately.
