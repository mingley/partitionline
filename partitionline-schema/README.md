# partitionline-schema

Scaffold companion for Confluent-compatible **wire framing** used with
[`partitionline`](https://crates.io/crates/partitionline).

**Not published** (`publish = false`). Workspace-excluded from the core
`partitionline` crate package. No librdkafka or OpenSSL; the default registry
feature uses Rustls with Ring, whose build uses a C compiler. Disabling default
features leaves dependency-free wire framing.

## Now

- `encode` / `decode`: magic byte `0` + big-endian schema id + payload
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

## Later (demand-gated)

Avro / Protobuf / JSON codecs — see
[`docs/schema-companion.md`](../docs/schema-companion.md) and survey
[#85](https://github.com/mingley/partitionline/issues/85).

```bash
cargo test --manifest-path partitionline-schema/Cargo.toml
```
