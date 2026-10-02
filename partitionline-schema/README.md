# partitionline-schema

Scaffold companion for Confluent-compatible **wire framing** used with
[`partitionline`](https://crates.io/crates/partitionline).

**Not published** (`publish = false`). Workspace-excluded from the core
`partitionline` crate package. No librdkafka or OpenSSL; the default registry
feature uses Rustls with Ring, whose build uses a C compiler. Disabling default
features leaves dependency-free wire framing.

## Now

- `encode` / `decode`: magic byte `0` + big-endian schema id + payload
- `protobuf`: bounded Confluent message-index framing (including the special
  `[0]` encoding), plus `Adapter<Codec>` for an explicitly selected serializer
  and known writer schema ID. No serializer dependency is selected implicitly.
- `avro` (opt-in feature): bounded Avro datum framing and `Adapter<Codec>` with
  explicitly selected writer/reader schemas and resolved named references.
  The application supplies its serializer; the feature adds no dependencies.
- `registry::RegistryClient` (default feature `registry`): bounded
  read-only Schema Registry lookups (by id, by subject/version, plus
  reference resolution). No registration or mutation APIs.

## Cache limits and freshness

Client clones share a cache scoped to their fixed URL and credentials. Separate
clients have separate caches. `RegistryClientConfig::cache` accepts a
`RegistryCacheConfig`; defaults retain at most 256 entries and 8 MiB of charged
key/schema storage, with at most 16 active distinct-key requests. IDs and pinned
versions are fresh for five minutes, `latest` for five seconds, and typed 404s
for one second. Other errors never enter the completed cache. Expiry is lazy;
eviction removes the least recently used entries. Oversized successful schemas
are returned without caching. `disabled()` disables completed caching while
keeping bounded request coalescing.

Same-key misses share a request; unrelated keys can proceed independently.
Dropping a follower leaves its owner alive. Dropping an owner closes its request
and lets a follower retry the read-only lookup within its original deadline.
Waiting for capacity, coalescing and retries all count toward `request_timeout`.
The client starts no background tasks.

Subject keys are limited to 1024 UTF-8 bytes by default, before path allocation
or I/O. Reference counts and subjects are checked before batch I/O and before
cache admission. Reference resolution is one level, preserves duplicates and
input order, and uses one overall batch deadline; cyclic declarations cannot
cause recursive traversal. A failed batch returns no partial vector, though
earlier successful lookups may remain cached.

`cache_stats()` returns entry, charged-byte, active-key and coalesced-waiter
counts without subject labels or credentials. Charged bytes include retained
string/vector capacities and inline entries; hash-table/allocator overhead and
caller-owned result clones/futures are separate. Applications must bound their
own callers and retained results. Configuration limits and maximums are in the
Rustdoc for `RegistryCacheConfig`.

## Protobuf bounds and schema selection

Generic five-byte `encode` / `decode` do **not** parse Protobuf indexes. Use
`protobuf::encode` / `protobuf::decode` for that format. Defaults bound the
complete frame to 1 MiB and its index path to 32 entries; `Limits::new` permits
6 bytes–64 MiB and 1–1024 indexes. Payload decoding borrows the original data;
only a bounded index vector is allocated. Encoding checks every limit before
reserving output. Negative fields, truncated/overflowing varints, invalid paths
and size errors are structured failures. Valid nonminimal varints are readable.

`Adapter` captures one codec's descriptor path and writer schema ID, rejecting
unknown IDs or different selected messages before invoking the decoder. It
asks for exact encoded length, allocates one bounded frame and gives the codec
only its payload slice. The application chooses and configures the serializer,
writer descriptor and imported schemas explicitly. Registry lookups/reference
resolution are separate; no lookup, registration, compatibility assumption or
recursive reference fetch is hidden in the adapter. Codec-internal scratch and
decoded-object/recursion limits remain the chosen codec's responsibility; the
frame bound is not an RSS or decoded-object bound.

The pinned [offline oracle](tests/oracles/protobuf/README.md) checks Confluent
8.1.0 indexes and Google protoc 3.21.12 payloads in both directions, including a
nested message with an imported schema. No live registry/broker is claimed.

## Opt-in Avro adapter

Enable `features = ["avro"]` to use `avro::Adapter<Codec>`. This is a codec
contract and bounded Confluent framing, with no built-in Avro serialization
library. Supply a known writer schema ID, separate writer/reader `Schema`
values, their already-fetched named references, and a caller-selected codec.
Construction checks input bounds before asking `Codec::resolve` to validate
and select the schemas once. Writer and reader reference sets are independent;
duplicate names within either set fail. Supply transitive references explicitly.
JSON validation, name resolution, defaults, union branches, promotions and
incompatible-schema decisions are the codec's responsibility. Schema Registry
lookup/reference fetching remains a separate read-only operation.

Encoding asks the codec for an exact writer-schema datum length and gives it
one bounded output slice, without a second adapter-owned payload buffer. Decode
borrows frame data, rejects unknown writer IDs before entering the codec, and
requires the codec to report complete consumption of one datum. Partial writes,
trailing data, overflows, oversized inputs and malformed headers are structured
adapter errors; schema/datum failures retain the codec's error type. The
five-byte frame with no payload is valid for a primitive Avro null. These are
raw binary datums, not Avro object containers or single-object encodings.

Defaults permit a 1 MiB complete frame, 2 MiB combined writer/reader schema
text (including every reference name and JSON), and 64 combined references.
`Limits::new` permits 5 bytes–64 MiB frames, 1 byte–64 MiB schema input and
0–1024 references. Schema text is borrowed and never copied by the adapter.
The selected codec must separately bound schema parsing, retained schemas,
recursion, scratch and decoded values; input bounds do not bound their memory
or process RSS. No registration, lookup or network operation is implicit.

The pinned [Apache Avro Python 1.12.1 oracle](tests/oracles/avro/README.md)
checks peer-produced bytes and actual Rust adapter output in both directions.
It covers writer/reader field order, int-to-long promotion, evolved named
references, added string/null/boolean defaults, null/present union branches,
UTF-8 and int32 boundaries, plus incompatible and missing-default failures.
The checked-in Rust codec handles only those fixture schemas; these tests do
not qualify a production built-in serializer, live registry or Kafka broker.

## Later (demand-gated libraries)

Built-in Avro / Protobuf / JSON serialization libraries — see
[`docs/schema-companion.md`](../docs/schema-companion.md) and survey
[#85](https://github.com/mingley/partitionline/issues/85).

```bash
cargo test --manifest-path partitionline-schema/Cargo.toml
cargo test --manifest-path partitionline-schema/Cargo.toml --features avro
```
