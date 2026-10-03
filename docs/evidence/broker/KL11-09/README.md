KL11-09 implements explicit bounded rolling storage for the ordinary broker data
Router. `produce::Config::segment_limits = Some(...)` selects it; the existing
default monolithic constructors, layout and seven-entry API profile remain.
Wrong or mixed layouts fail startup. There is no implicit migration.

One Catalog/Store blocking actor owns the partition handles and orders Produce,
catalog identity changes and Fetch/ListOffsets snapshots. Rolling and index work
run on that actor; long polls continue to wait outside it with their existing
admission permit. Confirmed appends or registered-handle poisoning notify before
reply, including a lost or canceled append receipt. Topic UUID and partition
index select paths; topic names never select storage filenames.

Each partition has one authoritative `PLSEGM01` manifest, contiguous sealed
`PLJRNL01` generations, one active journal and derived `PLSEEK01` bundles. This
is a Partitionline disk layout, not Kafka's disk format. Recovery checks every
selected record, physical boundary, offset, count, checksum and full-file
fingerprint before certifying sparse offset/time checkpoints. Missing or stale
derived bundles are reported and rebuilt. A valid bundle checksum does not make
its identity, position or timestamp trustworthy. Missing/corrupt authoritative
data and unknown layouts fail closed; only an incomplete final active entry is
repaired. A complete unacknowledged append can survive recovery.

A roll occurs before the next input crosses the soft file target or per-segment
entry cap. An input remains atomic under separate hard entry/file/scan/disk caps.
The old file is synchronized and certified; the seek bundle and new empty active
file are synchronized before an atomic manifest rename and directory sync. An
ambiguous failure poisons the handle. Explicit `replace_sealed` copies a complete
checked generation, publishes the new selection durably, then removes the old
files. It preserves all bytes and records; it is not retention or compaction.
The serving regression performs this maintenance between stopped owner sessions.

Timestamp hints use verified prefix maxima strictly below the requested time.
Equal and regressing record timestamps therefore cannot skip the first match.
Physical seeks read only the journal header and the chosen sparse predecessor;
every skipped and returned payload/entry counts against request work. Exhausting
hidden scan work returns an error, including after earlier outputs could fit.
Fetch still returns complete containing/following batches and preserves the
ordinary min-one/oversized-first-batch policy and transactional exclusions.

Limits bound segment count, per-segment entries, checkpoint stride, physical
file lengths including simultaneous old/new/staged generations, conservative
retained/transient index capacities and scan work. Store configuration charges
the sum of each historical/live partition's backend disk/index envelopes before
startup. At most one active data FD per store plus four transient actor FDs is
claimed; there is no sealed-file cache or index mapping. Payload work counts
payload bytes and entries; entry counts separately bound 32-byte entry headers
and 24-byte file headers. Recovery bounds complete file bytes per segment. One
bounded maximum-entry payload scratch is separate from index memory, alongside
normalized Produce work, one append copy and existing read input/output budgets.
Allocator bookkeeping, operator paths, fixed store metadata, caller output,
directory blocks and OS page/cache memory are separate. These are explicit
algorithmic envelopes, not a process RSS or physical allocation claim.

Behavioral tests cover real Router fixtures, actual TCP restart, assigned offsets
and payloads, lost receipt and append/delete wakes, hidden checkpoint work,
stale/corrupt indexes, incomplete tails, staging/disk/count/generation ceilings
and byte-preserving replacement. Private unit fault hooks cover 33 IO/process
exit histories and can retain interrupted/recovered files through
`PARTITIONLINE_SEGMENTS_FAULT_DIR`. They have no production fault environment
surface. Process exit skips destructors; it does not simulate physical power
loss, filesystem hardware durability or cross-process locking.

The separate root-owned [Apache oracle](oracle/README.md) executes actual pinned
4.1.2, 4.2.1 and 4.3.1 components. The Router test binds its six exact batches
and 15 applicable normal, from-offset-zero timestamp outcomes. Two sessions
compare 30 timestamp responses and 26 containing/whole first-batch Fetch
responses over real TCP across three segments, replacement and restart.
Produce's assigned leader epoch is explicitly rewritten outside record CRC.
Component negative timestamp searches, explicit search offsets and partial
`minOneMessage=false` slices remain component-only evidence; existing independent
wire goldens separately cover Kafka sentinels and ordinary Fetch semantics.

Development attempts, prerequisite failures and resource failures are retained
as development evidence. Final qualification requires the exact pushed source,
a complete Git archive including lint configuration, stable and Rust 1.85
behavior/strict checks, source identities before/after commands, and separate
independent receipts. No replication, automatic Apache rolling, transactions,
retention, compaction, benchmark, production or full Kafka qualification follows
from this task.
