# Accepted broker 403 proof: lossless publication packet

Qualified source: `403be1e3db073df86921d6fb21189f695c4f1eaf`.
This packet repackages accepted evidence; it runs no new broker/client tests and
does not expand the original scope. The original validation and normalizer
results retain 51 qualification commands and 10 separately counted generated
cache-maintenance commands, across 12 stable/Rust 1.85 feature-profile cells.

`restore-map.json.gz` maps every included original path, exact content SHA256,
byte count, and full permission mode (07777 mask) to a deterministic gzip object.
Content-identical original paths share an object; their names and modes remain
separate. Existing gzip source-audit files are wrapped as exact raw bytes, so
restoring them reproduces their original compressed bytes too. The map includes
directory paths/modes, including empty directories.

Run `python3 restore.py` to verify the map and all object contents without
inflating a full tree. Run `python3 restore.py --destination /an/empty/directory`
to restore namespaces `qualification/`, `normalization/`, and `origin/` beneath
that directory. Original absolute paths are recorded as provenance and are
never written automatically. Restore modes are applied after writing data.

All original qualification files outside `bin/` are included: original
validation, actual combined command stdout/stderr logs, disk-monitor samples,
source-integrity guards, cache ownership/retention receipts, and every non-ELF
file in all 12 closed runtime captures. These include membership histories,
counters, storage, WAL/segment/image data, snapshots and replication histories.
Original command exit values remain in validation.json; no invented split
stdout/stderr or per-command exit files replace the actual logs. The normalizer
has its own original command.log and command.exit. The origin receipt and
complete-source.json.gz are included unchanged.

`excluded-compiled-map.json.gz` records hashes, byte counts and full modes for
all 454 compiled-retention paths: 398 cache ELF-object gzips plus 56 selected
ELF paths (48 gzip files and 8 raw files). Thus storage contains 446 gzip ELF
files and 8 raw ELF files. Those payloads remain only in the original WORK tree. They are not in the
packet and restore.py does not restore them. Metadata describing their execution
and identity remains inside the unmodified raw proof.

`receipt.json` records packaging checks, exact input pins, all 61 actual command
log/disk hashes and exit results, and capture inventory. Its stronger packaging
path/full-mode checks do not rewrite the scope of older runtime source guards.
Raw runtime limits are preserved: process/IO histories do not establish physical
power-loss behavior, a complete Kafka cleaner, general KRaft qualification, or
public client runtime behavior. No performance claim follows from packaging.

Every stored blob is below the publication size guard. Files can be uploaded in
bounded waves; do not submit the entire packet as one oversized encoded API
request. All original proof/source/executable bytes remain untouched.
