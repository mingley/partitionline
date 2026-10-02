# Bounded record admission foundation

The independent broker's `records` module validates an entire caller-owned byte
slice before returning a borrowed `Validated` token. It performs no append or
filesystem operation. This is a foundation for a future Produce handler; it
adds no API/version advertisement and does not implement Produce policy,
acknowledgements, offset assignment, replication or producer deduplication.

Supported inputs are nonempty sets of ordinary uncompressed magic-2 batches.
Keys and values may be null or empty; values, keys and header values are arbitrary
bytes. Header keys must be valid UTF-8; empty and repeated header names are
accepted. CreateTime timestamp deltas may be negative, and the NO_TIMESTAMP
sentinel is structurally valid. Batch base offsets are checked independently:
client-produced batches can each start at zero. Signed offset/timestamp arithmetic
never wraps. Record offset deltas must be contiguous within each batch, and the
protected last delta must agree with its positive record count. A checked next
offset must remain representable as signed 64-bit Kafka offsets.

Validation checks the outer size before parsing, magic, CRC32C over all bytes
starting at batch attributes, metadata sentinels, attribute bits, protected
lengths/counts/offsets, exact 32/64-bit zigzag varint widths, record enclosure,
nullable key/value lengths, header lengths/counts, maximum timestamp, and complete
consumption of every record and batch. Declared sizes never cause an allocation.
CRC covers neither base offset, batch length, leader epoch nor magic; these
fields are independently validated. Checksums do not authenticate malicious
rewriting.

Typed `Unsupported` errors identify legacy magic 0/1, gzip/Snappy/LZ4/Zstandard,
idempotent producer metadata, transaction/control batches, delete horizons and
LogAppendTime. Known unsupported features are classified only after a valid
protected checksum for magic 2. The validator does not attempt to decompress or
validate semantics it cannot support. Unknown codecs/reserved attribute bits
and malformed producer sentinel tuples are structural errors.

`Limits` has positive independent budgets for input, complete batch, complete
record and individual field bytes, plus batch count, total records, headers per
record and total headers. Byte hard maxima are 64 MiB; count hard maxima are
one million. Defaults are 16 MiB input, 1 MiB batch/record/field, 1,024 batches,
65,536 records/total headers and 1,024 headers per record. Byte budgets include
encoded framing where applicable. Count checks precede corresponding loops;
the minimum record/header encoded sizes also reject impossible count claims.

The validation path uses borrowed slices, scalar counters and stack cursors. It
has no `Vec`, `Box`, `String`, collection, payload copy, index, recursion or other
heap allocation. It returns a five-machine-word borrowed token; batch iteration
also retains constant state and yields borrowed slices. Tests verify pointer
identity, object sizes and exact budgets. This is a source-level allocation
bound and checked borrowing evidence, not allocator instrumentation or a process
RSS guarantee. Caller-retained input, collected projections and concurrent
validations remain caller-owned resource budgets. CRC plus field/UTF-8 scans
perform work bounded by submitted bytes and configured count ceilings.

`RecordsOracle.java` uses pinned official Apache Kafka **4.3.1**
`MemoryRecords` builders and executes `validBytes`, `batch.ensureValid`, record
iteration and `record.ensureValid`. It emits six valid ordinary fixture sets,
11 upstream-supported feature cases, and 41 hostile/strict cases derived from
exact Apache-produced bytes. Protected mutations recompute CRC so deeper checks
are exercised. The native codec jars are external fixture-generation dependencies
only; no broker/client dependency or feature is added. Pins, distribution/source
provenance, commands, raw output, exact bytes/hashes and parser outcomes are in
`apache-oracle.json`, the three Apache logs and
`partitionline-broker/tests/fixtures/records/`.

Apache's low-level parser accepted 36 cases and rejected 22. Our admitted subset
accepts six and rejects the remaining 52 with typed errors. Eleven of those are
recognized unsupported features. Nineteen are intentional stricter admission
checks that Apache's low-level parser accepts: partial/trailing input (upstream
`validBytes` stops early), empty declared batches, incoherent offsets/metadata,
maximum timestamp mismatch, reserved attributes, non-UTF-8 header keys, certain
varint overflow/noncanonical encodings and timestamp overflow. These outcomes
are not claims that an Apache broker's `LogValidator` or Produce handler accepts
the same data. The retained executable oracle does not run a broker.

Two fresh Apache executions reproduced all 58 fixture bytes and exact parser
outcomes. The Rust tests cover this entire fixture table, all 74 truncations of
the complete basic batch, 11 incomplete trailing prefixes, all 424 bit flips in
its protected bytes, checksum precedence for seven unsupported features, all
eight independent budgets, positive/hard limit rejection, exact-size admission,
signed timestamp varlong boundaries, record enclosure and borrowed projections.
One composition test appends the fully validated original bytes and explicit
record count to the existing journal, verifies rejected inputs leave state
unchanged, and recovers the exact bytes/count after restart. This is composition
of two foundations, not integrated Produce or new durability qualification.

Stable and Rust 1.85 commands, exact tested source, counts, retained draft
failures and platform limits are recorded in `validation.json` after source
freeze. Existing journal qualifications and caller ownership/platform limits
continue to apply to the composition test. General compacted Fetch batches,
compressed admission, transactions, sequence fencing/deduplication and topic
policy require their own implementation and evidence before advertisement.
