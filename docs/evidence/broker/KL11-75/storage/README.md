# Ordinary compaction storage

This lane implements explicit caller-owned cleaning of a sealed ordinary prefix.
The actor supplies confirmed and protected boundaries; cleaning stops before the
active segment and never rewrites a segment overlapping the protected boundary.
Transactions, control batches, producer state, compressed stored data and
log-append timestamps remain unsupported. This does not close the parent full
compaction card or expose a Kafka cleaner API/background service.

Produce continues to use the existing dense admission validator. Stored reads
have a separate borrowed summary and checked record iterator, accepting sparse
offset deltas, validated ordinary delete horizons, canonical terminal empty
batches and zero physical batches. Actual retained counts are independent of the
original logical extent. Neither cleaning nor an empty physical entry changes
the confirmed logical floor or exclusive end.

The cleaner maps the latest offset of each non-null key within the complete
eligible round. It drops null-key records, retains latest keyed values and
initially retains latest tombstones with a delete horizon. An existing horizon
expires at equality. Only the round's terminal all-removed batch retains the
authentic 61-byte empty header; other removed batches disappear. Their enclosing
atomic entry keeps its original positive logical span even with zero payload.
Active and protected sealed suffixes do not enter the key map. The independent
Apache lane supplies actual `Cleaner.doClean` results and separately identified
caller-selected `filterTo` results; these are different kinds of reference.

The operation reserves its copied key arena/open-address table, explicit probe
budget, entry metadata, all prepared encoded outputs and publication/certification
scratch before creating any file. Each encoded entry has one exact reservation;
there is no repeated implicit vector growth. Remaining payload work bounds the
cursor before allocation. Validators and every projection pass reserve record
visits before parsing bodies. Header/CRC work is bounded by the admitted entry
bytes and entry counts. Exceeding a configured round, work or scratch ceiling
refuses the entire operation before publication. Large rounds may require a
different explicit policy or protected boundary; cleaning is not best-effort.

Dense V1/V2 data and prefix-retention victims keep their original meanings.
Explicit cleaning publishes V3 `PLSEGM03` and immutable `PLSPRS01` generations.
Older readers reject them. The new manifest has a distinct list of obsolete
changed generations, separate from prefix-retention victims. Its single atomic
selection chooses the complete old or complete new round. Recovery certifies all
selected survivors before unlinking exactly recorded obsolete names, then syncs
the directory and publishes a cleared list. Arbitrary unrecorded older files are
rejected under V3. Ambiguous write/publication/cleanup failures and source integrity
failures poison the handle; getters retain confirmed boundaries and serving
requires recovery. Policy/work/disk refusals preserve an operational handle.

The 24-byte sparse file header identifies its base and protects the header with
CRC32C. Each 32-byte `PLENTRY1` header protects payload length, original first
offset and positive logical span; a separate CRC32C protects the payload, including
the valid zero-length case. Complete sparse tails are immutable and never repaired.
Seek bundles use `PLSEEK02`, carrying the sparse descriptor kind and checked offset,
physical position and prefix-maximum checkpoints. Terminal empty batch maxima are
conservative seek/age hints; actual record scans determine timestamp answers.
Unknown/negative maxima still have no local age clock, as in V2.

`PARTITIONLINE_COMPACTION_CORPUS_DIR` optionally retains actual selected files
from the genuine three-release mixed, all-removed and null-key-only tests. Each
fresh case directory includes the manifest, data and seek files plus the observed
outcome and independent fixture identity, followed by a separate reopened-state
directory. The capture is bounded to 64 regular files, 1 MiB per file and 4 MiB
per state. Ordinary tests do not create these outputs. The independent checker
must inspect those bytes; a successful Rust assertion is not its input verdict.

The conservative V3 retained-index envelope includes simultaneous replacement
checkpoints, obsolete descriptors and manifest/directory buffers. Peak disk
reservation includes all old/new data and indexes plus both publication manifests.
Staged generations remain charged until directory-synchronized cleanup. The
blocking owner serializes the operation with append/read/catalog/retention; the
normal protocol advertisement is unchanged. These local formats and process/IO
fault tests do not claim Kafka disk compatibility or physical power-loss proof.

Development runs are provisional WORK-overlay checks. The final source pin,
complete-source compiler matrix, independent raw-state checker and public peer
receipts will be bound in this lane's final validation after source publication.
