# Partition journal foundation

This is a private experimental journal format, not a Kafka record-batch format.
Payloads are opaque and have a separately supplied positive logical record
count. Logical offsets advance by that count, rather than by payload byte length.
They are unsigned 64-bit values in this foundation; protocol integration must
apply Kafka's separate signed-offset and record-batch validation contracts.

All integers are big-endian. CRC32C uses the Castagnoli polynomial. Checksums
detect corruption; this format is not authenticated against hostile rewriting.

The 24-byte file header is:

| Byte offset | Length | Meaning |
|---|---|---|
| 0 | 8 | ASCII `PLJRNL01`, identifying format version 1 |
| 8 | 8 | Configured base logical offset |
| 16 | 4 | Reserved flags, currently zero |
| 20 | 4 | CRC32C of bytes 0–19 |

Each entry has a 32-byte header followed by its nonempty payload:

| Byte offset | Length | Meaning |
|---|---|---|
| 0 | 8 | ASCII `PLENTRY1` |
| 8 | 4 | Payload byte length |
| 12 | 8 | First logical offset |
| 20 | 4 | Positive logical record count |
| 24 | 4 | CRC32C of the entire payload |
| 28 | 4 | CRC32C of header bytes 0–27 |

The first entry must start at the file's configured base offset; subsequent
entries start exactly at the preceding entry's checked `first + record_count`.
Neither append nor recovery permits wrapping an offset.

`Journal` is synchronous and must run on a dedicated storage thread or bounded
blocking executor. A process-local ownership guard reserves normalized paths
before file creation; Unix inode identities also reject hard-link aliases.
Failed opens and dropped journals release these guards. No I/O or await occurs
while the registry mutex is held. Cross-process ownership remains the caller's
responsibility; this is not an operating-system file lock. Applications must
also bound their live journal count and retained fetch results. The registry
contains keys for live handles and releases its allocation when empty.

Initialization synchronizes the file header. Opening/recovery synchronizes every
accepted complete file, then its resolved parent directory, before success.
Append checks all budgets/offsets and reserves bounded index capacity before
writing. It writes the entry header and payload, calls `File::sync_data`, then
advances the retained index/offset. Any I/O failure in that sequence poisons the
handle and leaves its committed in-memory offset unchanged. A full entry from a
failed synchronization can still be recovered; the failed append's outcome is
ambiguous. Recovery synchronizes accepted entries before making them available.

Recovery checks the total file budget before scanning, verifies header checksums
before trusting declared lengths, and computes complete payload checksums using
4 KiB scratch storage. Its read loop retries interrupted calls and continues
through short reads; an early zero read inconsistent with the observed length
fails without repair. The index grows with a configured capacity ceiling.

A final partial entry header is repairable only when every available structural
field matches a possible next entry. A complete protected header whose payload
ends early is repairable only when the bounded available tail contains no valid
later entry header. Finding a later header fails closed rather than discarding
interior damage. This is conservative: a torn opaque payload that happens to
contain a valid later-offset header can be rejected instead of repaired. Full
header/payload checksum damage, impossible lengths/counts and offset gaps never
become a successful tail repair. A damaged or incomplete file header also fails
closed. Repairs truncate only the incomplete final entry and synchronize the
result before reporting the number of removed bytes.

Defaults bound payloads to 1 MiB, complete files to 1 GiB, the retained index to
65,536 entries, and each fetch's owned output to 16 MiB. Validated hard maxima
are 64 MiB payloads, 1 TiB files, one million index entries and 128 MiB fetch
output. Limits are positive; the file minimum fits its header and one one-byte
entry. Index capacity is bounded by entry count; allocation growth can transiently
require old and new buffers, bounded by twice the configured index capacity.
These are allocation/data budgets, not a process RSS bound or allocator-overhead
accounting. A recovery tail-inspection buffer is smaller than the already-checked
payload limit.

Fetch selects whole entries containing/following its logical offset. It charges
each returned `Entry`'s inline storage and payload length against the requested
and configured output cap before allocating. An oversized first entry returns
`FetchBudgetExceeded`; an oversized subsequent entry stops a successful bounded
fetch. A read/integrity error returns no partial vector and poisons the handle.
Applications should configure fetch output large enough for their allowed
single-entry payload plus inline metadata.

`sample.bin` is actual Rust-produced journal output with base offset 7 and two
payloads (`first-payload`: three records; `second-payload`: two records).
`verify-format.py` independently checks its byte layout and both CRC layers with
a dependency-free bitwise CRC32C implementation. This is custom-format evidence;
it does not establish Kafka wire interoperability.

The validation cases include deterministic write/zero-write/sync/truncate/read
faults, actual file reopening and child-process exit without destructors, all
45 incomplete final-entry prefixes, interior deletion/checksum/offset damage,
offset overflow, positive budgets, charged fetch output and simultaneous file
creation/alias ownership. Process-exit and synthetic torn-write tests do not
qualify physical power loss, hardware flush behavior, network filesystems,
replication, transactional durability or production recovery. Directory
synchronization must be supported by the configured filesystem; qualification
here is Linux x86_64, with stable Rust and Rust 1.85.
