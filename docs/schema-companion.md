# Companion crate design: `partitionline-schema` (WP-6.3)

**Status:** wire-framing **scaffold landed** under `partitionline-schema/`
(workspace-excluded, `publish = false`). Core `partitionline` `0.1.0` is on
crates.io — the publish gate for starting the companion is met. Do **not**
crates.io-publish this companion until survey
[#85](https://github.com/mingley/partitionline/issues/85) (or equivalent
adopter demand) justifies HTTP/codecs. Schema Registry support stays out of
the core client (`docs/gaps.md`).

Prove scaffold anytime: `bash scripts/check-schema-companion-scaffold.sh`

## Lookup client (KL05-23, landed)

The companion now ships a bounded read-only Schema Registry client
(`partitionline-schema::registry`, default feature `registry`):

- `GET /schemas/ids/{id}`, `GET /subjects/{subject}/versions/{version}`,
  and reference resolution. No registration, mutation, delete, or
  compatibility APIs exist in this module, and the companion never
  depends on the core `partitionline` client crate.
- `GET`-only HTTP/1.1 over `tokio` + `rustls` (Mozilla webpki roots, or
  a custom PEM CA bundle), full hostname verification, no insecure
  escape hatch. No `serde`/`url` dependencies: URLs and the small JSON
  responses are parsed by hand, mirroring the core OIDC token fetch.
- Auth: none, HTTP Basic, or bearer token. Credentials travel only in
  the `Authorization` header, are rejected in `base_url` userinfo, and
  are redacted from every `Debug` impl and error message (covered by
  redaction tests).
- Explicit bounds: per-connect/per-handshake timeout, an overall
  `request_timeout` per call (retries included), 1-8 attempts, doubling
  backoff with a ceiling (also capping honored `Retry-After`), 1 KiB -
  64 MiB response bodies, at most 1024 references per batch. Retried:
  429, 500/502/503/504, transient transport failures. Never retried:
  401/403/404, other statuses, TLS/JSON/size/config failures.
- Typed `RegistryError` (`../partitionline-schema/src/registry.rs`):
  `NotFound` (with registry `error_code`), `Unauthorized`, `Forbidden`,
  `RateLimited`, `Server`, `UnexpectedStatus`, `Malformed`, `TooLarge`,
  `Timeout`, `Tls`, `Transport`, `InvalidConfig`. Messages never embed
  response bodies, URLs, or credentials.
- `default-features = false` keeps the dep-free wire-framing core;
  `publish = false` is unchanged.

## Bounded cache (KL05-24, landed)

`RegistryClientConfig::cache(RegistryCacheConfig)` controls a cache shared by
clones of one client. Separately constructed clients keep credentials and cache
results isolated. Defaults: 256 completed entries, 8 MiB charged retained bytes,
16 active distinct keys, 1024 UTF-8 bytes per subject; five-minute IDs/pinned
versions, five-second `latest`, one-second typed 404s. Latest and pinned versions
use different keys. Other failures, malformed data, over-limit responses and
reference lists never become cached successes.

Eviction is least recently used, subject to both entry and byte limits. Expiry
is lazy on lookups/statistics; idle storage remains within those limits.
Oversized valid successes bypass caching without evicting existing entries.
Zero freshness disables that class; `disabled()` disables completed caching.
Hard maxima are 4096 entries, 64 MiB charged bytes, 128 active distinct keys,
4096 subject bytes and 24-hour freshness. Entry and byte limits must both be
zero or both positive. Charged bytes include retained string/vector capacities
and inline entry/Arc storage, excluding hash-bucket/allocator overhead and
caller-owned clones/futures. This is a cache budget, not a process RSS bound.

Same-key misses coalesce without holding an async lock during network I/O.
Dropping a follower preserves its owner's request; dropping an owner closes its
connection and wakes followers to take over the read-only lookup within their
original deadlines. Capacity waits, coalescing and retries share each call's
overall timeout. The cache creates no background tasks. Applications must still
limit their own caller count and retained results. `cache_stats()` exposes
aggregate counts without subjects, credentials or schema payloads.

Reference resolution remains one level with input order and duplicates
preserved, so cyclic declarations do not recurse. Input reference counts and
subject sizes are validated before I/O; returned reference lists are checked
before cache admission. The entire reference batch now has one
`request_timeout`, replacing KL05-23's per-entry budget. The first failure
returns an error without a partial vector; successful earlier individual
lookups may remain cached. Deterministic loopback tests cover cancellation,
coalescing, eviction, expiry and failed fetches; no live registry is claimed.

## Protobuf message indexes and adapter (KL05-26)

The dependency-free `protobuf` module adds the Confluent message-index path
after the five-byte header. `[0]` has its special one-byte zero encoding; other
paths encode a zigzag length followed by nonnegative zigzag indexes. Generic
five-byte framing alone is not Protobuf framing, and neither vendor framing nor
payload serialization establishes Kafka protocol conformance.

Complete frames default to 1 MiB and paths to 32 entries. Validated limits permit
6 bytes–64 MiB and 1–1024 indexes. Decode borrows payload data and owns only a
bounded index vector; encode checks size/depth before reserving output. Negative
counts/indexes, truncated or overflowing five-byte varints, invalid paths and
over-limit frames have structured errors. Valid nonminimal varints and explicit
length-one `[0]` remain readable; emitted `[0]` is canonical.

`Adapter<Codec>` selects one known writer schema ID and captures the codec's
message path. Unknown IDs or different messages fail before payload decoding.
Applications explicitly choose their serializer, writer descriptor and imported
schemas. The codec reports exact encoded length and writes into the adapter's
bounded slice, avoiding a second library-owned payload buffer. Registry lookup
and reference resolution remain separate operations with their own documented
bounds; no hidden registration, recursive fetch or compatibility policy exists.
Codec-internal allocations, decoded objects and descriptor recursion need the
chosen library's own limits. No serialization-library dependency or default
feature is added to either core or companion.

The companion's offline peer (`partitionline-schema/tests/oracles/protobuf/README.md`)
pins Confluent Schema Registry 8.1.0 source, Apache Kafka 4.1.0 distribution
ByteUtils, JDK 21.0.12.1 and Google protoc 3.21.12. It generates independent
frames/payloads and checks actual Rust-emitted output in the reverse direction,
including an imported-schema nested message. The fixture-only descriptor codecs
do not promise a production built-in Protobuf serializer. Live Schema Registry
and Kafka integration are not qualified by these offline checks.

## Why a companion

Operators often need Confluent-compatible wire (Avro / Protobuf / JSON Schema
with magic-byte + schema-id framing). Putting that in `partitionline` would:

- pull HTTP + schema caches into every Kafka deploy
- couple release cadence to registry protocol churn
- blur the “pure Kafka protocol client” story

A separate crate depending on published `partitionline` keeps the core small.

## Proposed crate layout (post-publish)

```
partitionline-schema/
  Cargo.toml          # depends on partitionline = "0.1", reqwest/rustls
  src/lib.rs          # SchemaRegistry client + Serde codecs
  src/wire.rs         # Confluent magic 0 + BE schema id + payload
  src/avro.rs         # optional feature `avro`
  src/protobuf.rs     # optional feature `protobuf`
  src/json.rs         # optional feature `json`
  examples/produce_avro.rs
  README.md
```

## Non-goals for v0

- Not a drop-in for `apache-avro` + custom framing alone without registry
- Not Kerberos to the registry (TLS + bearer/basic only in v0)
- Not embedding libavro C

## Acceptance when built

1. Lives in its own repo **or** a workspace member excluded from the core
   package `include` list.
2. No librdkafka or OpenSSL. The default Rustls/Ring registry feature needs a
   C compiler for Ring's build; dependency-free wire framing needs neither.
3. Round-trip example against a local Schema Registry + Kafka.
4. Documented as optional in `docs/ADOPTION.md` and core README.

## Trigger

Ship after:

1. `partitionline` on crates.io
2. Adoption survey [#85](https://github.com/mingley/partitionline/issues/85)
   shows Schema Registry as a real pilot blocker (or owner prioritizes it)
