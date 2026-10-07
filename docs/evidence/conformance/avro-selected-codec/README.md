# Selected Avro codec conformance

KL05-43 checks Apache Avro Rust 0.22.0 through the schema companion's
`avro::Adapter`, against Apache Avro Java 1.12.1. The codec is pinned with
compression features disabled and used only in tests. Applications still
select their serializer; the companion is unpublished.

Nine Java-produced frames are decoded with the real Rust reader. The real
Rust writer emits byte-identical frames, which Java reads using both the
writer and evolved reader schemas. The cases cover reordered fields,
int-to-long promotion, evolved `common.Metadata` references, boolean/string/null
defaults, nullable string unions, UTF-8, int32 boundaries, primitive null and
boolean, long-to-double rounding, and float-to-double promotion.

Both peers reject incompatible readers, required fields without defaults,
missing references, truncation, invalid union indexes and trailing bytes.
Rust also checks reference labels, invalid UTF-8, invalid writer values,
unknown schema IDs, bad magic, frame/schema bounds and codec budgets.
Trailing-data rejection belongs to the frame wrappers: raw Avro datum readers
can leave unread bytes.

## Backend findings

Rust Avro 0.22.0 returns null after a string body's `UnexpectedEof`. With a
nullable reader, that can accept a truncated datum. The test bridge's
`StrictInput` retains EOF failures and rejects the result before returning it.
The failing unguarded runs are retained under `development/`. This profile
qualifies the guarded bridge, not the unwrapped backend for hostile input.

Java Avro's fast reader throws `ArrayIndexOutOfBoundsException` for the invalid
union index. The peer records that rejection explicitly; it makes no claim
that Java reports every malformed input as an Avro-specific exception.

## Resource profile

| Owner | Limit or behavior |
|---|---|
| Adapter | 4096-byte complete frame, 16384 combined schema/reference text bytes, eight combined references; checks before codec work |
| Schema bridge | JSON nesting at most 32 before parsing; parsed expanded depth at most eight, 128 nodes, 32 fields per record, four branches per union |
| Supported schema family | Primitives, finite records, unions and explicitly supplied named record references; arrays, maps, logical types, enum/fixed and recursive schemas fail this profile |
| Production writer | Count-only `Write` sink obtains exact encoded length; the writer then fills the adapter's exact slice without a second encoded payload buffer |
| Backend decoder | Process-wide `max_allocation_bytes(4096)` guard; construction refuses an already-established different guard |
| Datum bridge | Depth/node checks and 4096 aggregate string/byte bytes before encoding and after decoding |
| Java process | 128 MiB JVM heap, 30-second deadline, explicit exit/wait receipt; no broker or registry process |

The backend allocation guard covers wire allocations, not all parsing,
resolution defaults, name maps or allocator overhead. Schema text and finite
expansion limits constrain that work separately. The decoded aggregate check
is an acceptance check after construction. Temporary writer/reader values,
caller-owned values, defaults, retained schemas and backend scratch remain
separate allocations. No measured RSS ceiling or aggregate allocation quota
is claimed.

## Reproduce

Use the latest stable Rust, Python 3.11+ and a Java 21 runtime with its compiler
module. The peer runner downloads six hash-pinned Maven jars into the chosen
cache, verifies SHA-256 and records Java version, command, streams, PID, exit
and artifact hashes.

```bash
rustup update stable
mkdir -p work/avro-selected
PL_AVRO_SELECTED_OUTPUT="$PWD/work/avro-selected/default" \
  cargo +stable test --locked --manifest-path partitionline-schema/Cargo.toml --features avro
python3 tests/conformance/run-avro-selected-codec.py \
  --rust-output work/avro-selected/default \
  --peer-cache work/avro-peer-cache --report work/avro-selected/default-peer.json
PL_AVRO_SELECTED_OUTPUT="$PWD/work/avro-selected/all-features" \
  cargo +stable test --locked --manifest-path partitionline-schema/Cargo.toml --all-features
python3 tests/conformance/run-avro-selected-codec.py \
  --rust-output work/avro-selected/all-features \
  --peer-cache work/avro-peer-cache --report work/avro-selected/all-features-peer.json
```

The `schema` CI job performs both cells and retains Rust output and peer
receipts even on failure. Local evidence is bound to the uncommitted candidate
by `source-input-hashes.json`; no remote CI pass is claimed. The user's
2026-10-05 instruction replaces this card's former Rust 1.85 checks with
latest-stable checks. Historical evidence keeps its original compiler versions.

Plain default, default plus Avro, all features and dependency-free Avro tests
pass. The codec/Java comparison is required in the latter three cells. A
broader CI-tooling sweep also exposed existing broker registry checksum drift:
`partitionline-broker/src/metadata.rs` equals Git HEAD but no longer matches
its registry pin. That result is retained separately and does not qualify the
broker registry. No live Schema Registry, Kafka broker, publication,
arbitrary-schema conformance, throughput or fastest-client/server claim is
made by this task.
