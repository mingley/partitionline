# KL11-67 bounded broker codec normalization

The optional `codecs` module normalizes ordinary magic-2 gzip, Snappy, LZ4 and
Zstandard batches, then feeds the entire normalized input to the unchanged
`records::validate`. It returns no result until every batch and record is admitted.
Only compression bits, batch length and CRC32C change. Offsets, timestamps,
producer metadata, keys, values and headers retain their exact bytes. Ordinary
uncompressed input takes a borrowed, allocation-free output path. The module does
not append, assign broker offsets, implement Produce/Fetch policy, advertise APIs
or establish production/performance qualification.

## Exact source and dependencies

Source `a5ca44afab5460bf43161365c005cd6b3769aff2`, tested base
`37d72a18eef933f9005b9f7453763945882f104e`. Source SHA256 and exact gate commands
are in `final-a5ca44af/validation.json`; the independent review binds the same Git
objects in `independent-review-by-open-loop.json`.

The independent broker feature is non-default and activates `flate2 1.1.10`
(defaults off; `rust_backend`, `runtime_detection`), `snap 1.1.2`, `lz4_flex 0.14.0`
and exactly `zstd-rs 0.1.0`. Gzip uses miniz_oxide 0.9.1, not native zlib or
zlib-rs. Zstandard follows the vetted KL05-02 decision: archive SHA256
`41514ccc30389f95bb9e6ab6d5634f17121c7b4fead6722a5fceec1c7891d78f`, VCS commit
`bac4c37d86e5307537c145823a106f7ed8f386d7`, MIT OR Apache-2.0, declared MSRV 1.85,
unsafe forbidden and no runtime/build dependencies. Its short maintenance history
remains the adoption risk documented in that decision; no upgrade or default
policy change occurs here.

`dependency-proof.json` records licenses, MSRV, registry checksums, enabled
features and inspected backend source hashes verified against locked archives.
The active broker default graph has 17 packages and is identical to the claim
base. Enabling only codecs adds nine packages, for 26 total. All prior broker lock
versions remain present, including libc 0.2.189. Client manifest/lock and active
graph, `records.rs` and `journal.rs` are unchanged. The optional codec families
are absent from the default graph; native codec chains, dictionaries and hidden
runtime backends are absent from the active codec graph. Existing optional TLS
uses its separately approved ring backend when TLS is selected.

Cargo metadata overapproximates flate2's weak optional `zlib-rs?/std` edge and
platform-only nodes. The proof therefore uses retained `cargo tree` normal/build
activation on x86_64-unknown-linux-gnu and separately reports metadata-only nodes.
The first overcounting proof assertion is retained in
`dependency-proof-first-failure.txt`; no production dependency changed to fix it.

## Resource and framing contract

Positive codec limits independently bound encoded input bytes, encoded batch
bytes, batch/record counts, complete decoded batch bytes, complete normalized
input bytes, advertised window/block size, workspace and aggregate codec units.
Byte limits have a 64 MiB hard maximum, counts one million, window 8 MiB and
workspace 256 MiB. Defaults are 16 MiB input/normalized bytes, 1 MiB encoded and
decoded batch bytes, 1,024 batches, 65,536 records/units, 4 MiB window and 32 MiB
workspace. The existing record limits independently bound records, fields and
headers after decoding. No idempotent, transaction, control, delete-horizon or
LogAppendTime semantics become ordinary success.

Complete outer input preflight checks encoded lengths, batch CRCs, attributes,
producer sentinel rules, record counts and codec headers before output allocation
or decoder construction. Snappy chunk lengths and advertised output lengths,
LZ4 frames/blocks and Zstandard frames/blocks/windows/content sizes are preflighted
without allocating indexes. Gzip's later member boundaries and stream checksum
can only be checked during bounded inflation; each member is charged before its
decoder is constructed. Non-gzip units remain charged from preflight, avoiding
double counting on a boundary re-scan and preventing a mixed-codec budget bypass.
The pinned mixed regression needs five units: two gzip members, one Zstandard
frame, its data block and JNI's final empty block.

Output is reserved once before decoding. Its requested capacity is the sum of
compressed batches' decoded ceilings and exact uncompressed batch lengths,
clipped to the aggregate normalized ceiling, plus the pinned Zstandard decoder's
32 writable slack bytes. A single compressed batch defaults to a 1 MiB + 32 byte
reservation rather than 16 MiB. Output capacity plus the maximum sequential
backend scratch allowance must fit workspace both before reservation and before
decoder construction. The allowances are 512 KiB for gzip, no additional output
scratch for raw Snappy, `3 * advertised LZ4 block size + 64 KiB` for the source and
worst linked-block destination buffers, and 1 MiB for the pinned Zstandard entropy
tables/literal scratch including capacity-growth margin. Zstandard retains
history in the already bounded output vector rather than a separately allocated
advertised window. Scratch contexts run sequentially and are dropped per batch.

These are admitted vector capacity/source bounds, not allocator metadata, stack,
RSS or a global OOM recovery guarantee. The module's output reservation is
fallible; upstream scratch allocations still use ordinary infallible Rust
allocation. The caller must budget retained encoded inputs, normalized results
and concurrent calls. Work is bounded by encoded/output bytes and record/unit
counts, not a wall-clock deadline. No recursion or per-record allocation/index is
introduced in the normalizer. Every error drops partial output and decoder state;
Zstandard's partial-on-error behavior never exposes a partial admission token.

Accepted framing is ordinary gzip with flags zero, raw or version-1 compatible-1
Xerial Snappy, standard LZ4 with block sizes through 4 MiB, and standard Zstandard
without dictionary descriptors. Gzip optional names/comments/extras/FHCRC are
rejected before header-string allocation. Dictionaries, skippable/legacy frames,
reserved/unused descriptor bits and unconsumed compressed bytes are rejected.
Concatenated gzip/LZ4/Zstandard streams within a batch and multiple batches are
supported under aggregate budgets. These are explicit local choices, not an
assertion of complete codec/broker equivalence to upstream.

## Actual independent fixtures and differences

`CodecsOracle.java` and `run-apache-oracle.py` use official pinned Apache Kafka
4.3.1, source commit `26b251a451ce941d3d7a55e6487bcb7f16b5ad48`, client jar SHA256
`dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e`.
Classpath SHA256 pins for slf4j 1.7.36, Snappy JNI 1.1.10.5, lz4-java 1.8.0 and
Zstd JNI 1.5.6-4 are retained with the Apache distribution provenance. Native
tools are fixture producers/parser peers only, never broker/client dependencies.
The executed JDK is 21.0.12.1, with a 64 MiB heap and 45-second per-command bound;
fixtures are capped at 64 KiB and record iteration at 32.

Two fresh executions from the exact committed oracle reproduced all 44 fixture
bytes and complete parser outcomes/histories. `final-apache-a5ca44af/` retains
all three command logs and `apache-oracle.json`. Apache accepted 26 and rejected
18; Rust admits 16 and rejects 28. Actual codec builders create null/empty/binary
keys/values, repeated/empty/Unicode header names, nullable header values, negative
timestamp deltas, independent batch offsets, concatenated streams and 32 KiB
expansion cases. Normalized bytes match the independently generated uncompressed
reference exactly; those complete bytes have an independently executed Apache
history, including keys/values, offsets, timestamps and headers. The mixed
plain+four-codec input normalizes to five exact independent plain batches.

Expected Rust classification and actual Apache parser outcome are separate
fields, preserving differences rather than rewriting the oracle:

- Apache accepts trailing protected bytes for gzip, Snappy and LZ4; Rust rejects
  them as incomplete/trailing codec framing.
- Apache's parser accepts actual idempotent/transactional compressed records;
  Rust keeps those unsupported ordinary-admission semantics rejected.
- Rust joins split concatenated LZ4 streams to the exact independently verified
  plain history; Apache's compressed record parser rejects that split fixture.
  This supported extension means the framing relation is not uniformly stricter.
- All four protected count-bomb Apache outcomes are actual `OutOfMemoryError`
  under the explicitly bounded JVM heap. They are resource failures, not proof of
  a safe semantic count rejection. Rust checks the declared count budget before
  decoder/output allocation. Raw Snappy is accepted by both implementations.

Neither the oracle nor this adapter runs an Apache broker, LogValidator or
Produce handler. Independent primitive parsing does not qualify broker policy,
durability, replication, acknowledgement behavior or performance.

## Gates, hostile controls and retained development failures

Exact stable rustc 1.99.0 and MSRV 1.85.0 passed 18 recorded commands, including
two package-only cleans. Each lane passed default 111, codec-enabled 123 and all
features 155 ordinary test executions, strict format, all-target/all-feature
Clippy and rustdoc with warnings denied, and default/all-feature doctests (zero
doctests). There are 778 ordinary test executions across both lanes/configurations;
the twelve distinct codec regression tests repeat in the enabled/full lanes.
Builds use CPU 0-2,4, one job, no incremental compilation and no debug information
for development/test profiles.

The codec tests cover all 44 actual fixtures, exact-byte normalization/borrowed
pointer identity, every truncation of all four compact compressed batches, 240
incomplete trailing-batch prefixes, corrupted outer CRC precedence, count bombs
before malformed codec parsing, every independent limit/hard ceiling, exact and
one-byte-below output bounds, real expanding streams, decoded header limits,
unsupported metadata and malformed later batches, gzip optional-header rejection,
dictionary/skippable/legacy/reserved forms, huge Zstandard window/content-size
descriptors, LZ4 window rejection and Snappy declared-output bombs. A failed call
followed by a fresh successful call verifies failure isolation without mutating
caller input or any journal.

`development-failures.json` and raw development logs preserve the first test
helper borrow-check failure, corrected aggregate-unit review finding, the initial
four-unit test expectation that omitted JNI's final empty block, strict test-helper
lint failures, the transient development libc 0.2.190 graph before root restored
0.2.189, and the metadata graph overcount. No failing command is presented as a
passing final gate. `checksums.json` seals the evidence, including the independent
read-only review receipt; all final source/fixture pins are immutable Git objects.
