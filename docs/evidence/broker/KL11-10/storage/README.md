# Bounded rolling-storage retention

This lane implements the storage hooks behind the explicitly enabled retention
Router. The Store and wire integration are owned by the separate runtime lane.
The immutable final-source execution and independent receipts will be recorded
after the coordinator pushes the complete implementation. Development runs use
a complete `81b490f310bb3dbe4e89e4e140ef658e29fdf9e7` archive with the four
claimed source/test files overlaid; they are not committed-source qualification.

`run-final.py` extracts the complete supplied Git commit, checks the exact file
set and every Git blob before and after each command, and uses one cleaned
Cargo target across source/toolchain changes. Its 19 commands cover locked
stable/Rust 1.85.0 default/all-feature tests, strict Clippy, rustdoc, doctest and
formatting. The four capture directories are `stable-default`,
`stable-all-features`, `1.85.0-default` and `1.85.0-all-features`. They retain
ordinary/retention/rolling reports and fault files together with source-bound
snapshot and replication captures. Live retention/fetch executables and
compressed snapshot replay executables are preserved before cache cleanup.
The independent live peers and file/history checkers consume those captures;
their receipts are separate from compiled test success.

`Partition::log_start_offset()` is a monotonic logical boundary. A requested
offset inside an atomic input preserves that whole input physically. Reads
below the logical boundary fail; reads at the boundary can return the containing
batch's earlier records. The wire owner filters timestamp searches at the
logical boundary and reports the stored log start independently of physical
segment bases.

Every operation checks `DeletionGuard` against the current logical start and
actual synchronized end. Both the confirmed high watermark and `retain_from`
must lie in that range. A request above the confirmed watermark is out of range;
a request within that watermark but above the protected boundary is refused as
protected records. Ordinary RF1 integration supplies the actual synchronized
end for both bounds. These values do not assert a replicated quorum or ISR.
Negative wire sentinels are resolved by the wire owner. Requests at/below the
current floor return that floor without lowering it.

`RetentionPolicy` selects an offset-ordered prefix, capped by a positive number
of segments per explicit call. Age uses the strict comparison
`now_ms - verified_max_timestamp > retention_ms`. Entirely negative or unknown
timestamp segments are retained by age. Apache uses filesystem modification
time for such segments; the local policy deliberately supplies no clock from
mutable filesystem metadata. Guarded size retention and explicit deletion can
still remove them. Size counts record payload bytes and selects a whole segment
only when its removal leaves at least the configured byte target. Age or size
can make a safe prefix eligible; an ineligible/protected prefix blocks later
segments. Threshold zero is valid, absent thresholds disable that criterion.
There is no background sweep timer.

A fully deleted/eligible nonempty active file is certified, closed and replaced
by a synchronized empty active at the original exclusive end. The manifest
selects that empty successor before the old active becomes a cleanup victim.
This allows the one-segment configuration to reclaim dead bytes and append
again. An overlapping batch/segment is never split.

## Versioned publication and recovery

Opening or ordinary appending preserves `PLSEGM01`. Explicit retention publishes
`PLSEGM02`; a V1-only reader rejects that layout instead of losing the floor.
New readers support both versions. Monolithic journals remain unchanged and
reject retention with `RetentionUnsupported`.

The V2 big-endian manifest has a 64-byte header: eight-byte magic, then revision,
immutable origin, physical survivor base, logical floor, active base and active
generation as six `u64` values, followed by selected and victim counts as `u32`.
Selected descriptors precede victim descriptors; each retains the existing
56-byte protected descriptor format. A final CRC32C covers the whole preceding
manifest. Selected descriptors are contiguous through the active base. Victims
form a contiguous prefix ending at the new physical base, below the logical
floor. Their generation, lengths, entry/range bounds and total count are checked
before cleanup. The logical floor is checked against the actual recovered end.

Publication writes and synchronizes a temporary manifest, renames it atomically
and synchronizes its directory. Only then are recorded victim data/seek files
unlinked. Cleanup synchronizes the directory and atomically publishes a manifest
with the victim list cleared. Any ambiguous failure poisons the live handle.
Recovery validates the selected active/sealed data first, then resumes only
recorded victims; already absent victim files are idempotent cleanup. Missing
selected data and impossible checksum-valid victim descriptors fail closed.
Success means floor publication and cleanup have both completed.
The scalar floor getter retains the last confirmed publication separately from
the prospective manifest. A failed first publication cannot expose a boundary
that decreases on reopen. Once first publication succeeds, later cleanup
failures retain that confirmed boundary; serving still refuses poisoned handles.

The selected segment count retains the configured ceiling. Replacement/active
retirement can temporarily retain one additional old physical file, charged to
the disk envelope until unlink. Preflight includes the new empty journal and
both larger-manifest publication peaks. Retained victim descriptor capacity and
manifest buffers are included in the index envelope. The existing bounded
scan, one-entry validation scratch and active-plus-four-transient-file envelope
apply; Store must aggregate those per-partition limits. No whole-log payload
materialization is introduced.

## Behavioral and fault scope

Tests exercise guards, containing inputs, V1/V2 migration, monotonic restart,
strict age/size boundaries, nonmonotonic/unknown timestamps, capped prefix
progress, active reclamation and continued writes under a one-segment ceiling.
The fault matrix has 23 injected I/O failures and 23 actual process exits per
lane: eleven prefix-deletion stages and twelve active-retirement stages. Each
history first acknowledges floor 1 after five synchronized BASIC records, then
interrupts deletion through 3 or 5. Interrupted and recovered physical file
copies carry the original request, guards and expected recovered floor. Tests
verify the previously acknowledged floor, exact surviving payloads, stable end,
cleanup completion and successful subsequent append.
An additional 23 injected getter/reopen scenarios verify that the reported
confirmed floor never decreases. The failing-first regression and original
checkpoint source are preserved under `development/floor-getter/`.

These finite local process/file histories do not establish physical power-loss
behavior, multi-replica retention safety, compaction, exhaustive crash scheduling
or production qualification. Independent Apache and custom-format decoding
receipts are separate from the Rust histories.
