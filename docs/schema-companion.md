# Schema companion

`partitionline-schema` provides bounded Confluent framing, read-only Schema
Registry lookup/cache and adapters for explicitly selected Protobuf, Avro and
JSON Schema codecs. It is separate from the Kafka client and remains unpublished
(`publish = false`). Applications supply their serializer/validator; the selected
production codec tests qualify finite offline families.

Run `bash scripts/check-schema-companion-scaffold.sh` to check the crate boundary.

## Lookup client

`partitionline-schema::registry` is a read-only Schema Registry client.
It is the default `registry` feature. The companion does not depend on
the core `partitionline` crate. `default-features = false` keeps the
wire-framing core with no registry dependencies. `publish = false` is
unchanged.

The client sends `GET /schemas/ids/{id}`,
`GET /subjects/{subject}/versions/{version}`, and resolves references.
It does not register, change, delete, or check compatibility.

Transport is HTTP/1.1 with tokio and rustls. Roots are Mozilla's, or a
PEM CA bundle you supply. Hostnames are verified. There is no switch
that skips that check. URLs and the small JSON bodies are parsed by
hand, the same way the OIDC token fetch works. The crate does not use
`serde` or `url`.

Auth is none, HTTP Basic, or a bearer token. The credential is only the
`Authorization` header. A `user:pass@` userinfo on `base_url` is
rejected. `Debug` and errors redact the credential. They also omit
response bodies and URLs.

| Bound | Value |
|---|---|
| Attempts | 1 through 8, with doubling backoff and a ceiling |
| `Retry-After` | Honored, and still capped by that ceiling |
| Body | 1 KiB through 64 MiB |
| References in one batch | At most 1,024 |
| Deadline | Connect and handshake timeouts, plus one `request_timeout` for the whole call, retries included |

Retries cover 429, 500, 502, 503, 504, and a transient transport
failure. 401, 403, 404, any other status, and TLS, JSON, size, or
config failures are not retried.

`RegistryError` in `partitionline-schema/src/registry.rs` is `NotFound`
(with the registry `error_code`), `Unauthorized`, `Forbidden`,
`RateLimited`, `Server`, `UnexpectedStatus`, `Malformed`, `TooLarge`,
`Timeout`, `Tls`, `Transport`, or `InvalidConfig`.

## Bounded cache

`RegistryClientConfig::cache` shares one cache among clones of one client.
A second client keeps its own credentials and its own cache.

| Setting | Default | Hard maximum |
|---|---|---|
| Completed entries | 256 | 4,096 |
| Charged retained bytes | 8 MiB | 64 MiB |
| Active distinct keys | 16 | 128 |
| Subject length | 1,024 UTF-8 bytes | 4,096 |
| ID or pinned version | 5 minutes | 24 hours |
| `latest` | 5 seconds | 24 hours |
| Typed 404 | 1 second | 24 hours |

`latest` and a pinned version use different keys. Entry and byte limits
are both zero or both positive. Zero freshness turns that class off.
`disabled()` turns completed caching off.

Eviction is least recently used, and it honors both the entry cap and
the byte cap. Expiry is checked on lookup and on statistics. Idle
storage stays inside those limits. A valid response that is too large
is not cached, and it does not evict what is already there. Failures
other than a typed 404, malformed data, and over-limit reference lists
are not stored as successes.

Charged bytes include retained string and vector capacity and the inline
entry storage. They exclude hash buckets, allocator overhead, and clones
or futures the caller still holds. This is a cache budget, not process
RSS.

Misses for the same key share one request. The cache does not hold an
async lock across the network read. Dropping a follower leaves the
owner's request running. Dropping the owner closes that connection and
wakes a follower to finish the read-only lookup inside the follower's
original deadline. Capacity waits, coalescing, and retries share the
call's `request_timeout`. The cache starts no background task. The
application still limits how many callers it creates and how long it
keeps results. `cache_stats` returns counts only. It does not return
subjects, credentials, or schema bytes.

References resolve one level. Input order and duplicates are kept, so a
cycle does not recurse. Counts and subject sizes are checked before
I/O. The returned reference list is checked before it enters the cache.
The whole reference batch uses one `request_timeout`. The first failure
returns an error and no partial vector. Lookups that already succeeded
can stay cached. Loopback tests cover cancellation, coalescing,
eviction, expiry, and failed fetches. No live registry is claimed.

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

The companion's [offline peer](https://github.com/mingley/partitionline/blob/a5c0a7f12d8b546ea61267e5fd681e944edcf35f/partitionline-schema/tests/oracles/protobuf/README.md)
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
