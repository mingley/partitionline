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

## Protobuf message indexes

The `protobuf` module is dependency-free. It writes the Confluent message
index after the five-byte header. The path `[0]` is the special one-byte
zero. Any other path is a zigzag length and then nonnegative zigzag
indexes. A five-byte header by itself is not Protobuf framing. Vendor
framing and payload serialization are not a Kafka protocol claim.

A complete frame defaults to 1 MiB. A path defaults to 32 entries.
Validated limits are 6 bytes through 64 MiB, and 1 through 1,024 indexes.
Decode borrows the payload and owns only the index vector. Encode checks
size and depth before it reserves output. A negative count or index, a
truncated or overflowing five-byte varint, an invalid path, and an
over-limit frame are structured errors. A valid non-minimal varint, and
an explicit length-one `[0]`, still decode. The encoder emits the
canonical one-byte `[0]`.

`Adapter<Codec>` selects one known writer schema id and records that
codec's message path. An unknown id, or a different message, fails before
payload decoding. The application chooses the serializer, the writer
descriptor, and imported schemas. The codec reports the exact encoded
length and writes into the adapter's bounded slice. There is no second
buffer owned by this crate. Registry lookup stays a separate call. There
is no hidden registration, recursive fetch, or compatibility check.
Allocations inside the chosen codec, decoded objects, and descriptor
recursion use that library's limits. Neither the core client nor the
companion gains a serialization-library dependency or a default feature
for it.

The [offline peer](https://github.com/mingley/partitionline/blob/02ce806d7ea4970897d51604e71e6a1fa069ca4c/partitionline-schema/tests/oracles/protobuf/README.md)
pins Confluent Schema Registry 8.1.0 source, Apache Kafka 4.1.0
`ByteUtils`, JDK 21.0.12.1, and Google protoc 3.21.12. It builds frames
and payloads on its own and checks Rust output in the other direction,
including a nested message from an imported schema. Those fixture codecs
are not a built-in production Protobuf serializer. The offline checks do
not qualify a live Schema Registry or a live Kafka cluster.

## What this crate is

Confluent Avro, Protobuf, and JSON Schema framing stays out of the core
client so a Kafka deploy does not pull an HTTP cache, and so registry
changes do not set the client's release. This crate does not depend on
`partitionline`. Wire framing has no dependencies. The `registry` feature
uses tokio and rustls, not `reqwest`.

| Path | Role |
|---|---|
| `src/wire.rs` | Magic byte 0, big-endian schema id, payload |
| `src/registry.rs` | Read-only lookup client and cache |
| `src/protobuf.rs` | Message indexes and `Adapter` |
| `src/avro.rs` | Optional `avro` feature, no extra dependencies |
| `src/json_schema.rs` | Optional `json-schema` feature, no extra dependencies |

The application supplies the serializer. This crate does not embed
libavro. Registry auth in this version is TLS plus bearer or basic. It
does not speak Kerberos to the registry. It is not a drop-in for
`apache-avro` plus a hand-rolled frame and no registry.

The crate lives in this repository, is excluded from the core package,
and is unpublished. It does not depend on librdkafka or OpenSSL. Ring's
build needs a C compiler when the registry feature is on. The adoption
page and the core README name it as optional. A live round trip against
a local Schema Registry and Kafka is still separate from the offline
checks above. Pilot demand is tracked in
[issue 85](https://github.com/mingley/partitionline/issues/85).
