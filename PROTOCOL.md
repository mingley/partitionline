# Protocol

**Reuse [`kafka-protocol` 0.18](https://crates.io/crates/kafka-protocol).**
Do not write a custom Kafka codec. Do not regenerate wire types
(`MetadataRequest`, `ProduceRequest`, `FetchRequest`, `ApiKey`, headers,
tagged fields, flexible/compact).

```toml
kafka-protocol = { version = "0.18", default-features = false, features = ["client"] }
```

Default features of that crate enable C `lz4` and C `zstd`. We never turn
those on. gzip/snappy in kafka-protocol are pure Rust; we still keep them off
and bring `lz4_flex` / `ruzstd` (decode) / `flate2`+`zlib-rs` / `snap` ourselves
via `RecordBatchEncoder::encode_with_custom_compression` and
`decode_with_custom_compression`. No C/FFI anywhere.

## What we take from kafka-protocol

`messages::*`, `ApiKey`, `Encodable` / `Decodable`, request/response headers,
tagged fields, flexible and compact types, `Record` + magic-v2
`RecordBatchEncoder` / `RecordBatchDecoder`.

## Named gaps (document; do not rewrite the crate)

| Gap | What we do |
|---|---|
| Schema pin is **Kafka 4.1.0**, not 4.3 | Live with it. `DescribeLogDirs` stops at v4 (no KIP-1066 v5). Streams keys 88–89 are absent — we are not a Streams engine. |
| Owned-only decode ([issue #42](https://github.com/tychedelia/kafka-protocol-rs/issues/42)) | Known. Not a reason to regenerate request types. |
| Unbounded `Vec::with_capacity` from wire counts ([PRs #152](https://github.com/tychedelia/kafka-protocol-rs/pull/152) / [#161](https://github.com/tychedelia/kafka-protocol-rs/pull/161)) | Cap frames we allocate (`frame::MAX_FRAME`). Do not fork their decoder. |
| Produce records go through intermediate `Bytes` ([issue #104](https://github.com/tychedelia/kafka-protocol-rs/issues/104)) | If the librdkafka bench requires it, a **local** `RecordBatch` encode/decode module is the only custom wire we write. We do not regenerate `ProduceRequest` / `FetchRequest` / `MetadataRequest`. |
| C compression features | Disabled. Custom hook + `lz4_flex` / `ruzstd`. |
| `ruzstd` is decode-only | zstd produce is incomplete; lz4 is the compressed bench row. |
| SASL / TLS / KIP-848 | Types exist in kafka-protocol. Not wired in this client yet. Flagged, not silently skipped. |

## Category

Crowded: `rdkafka` 0.39.0 (FFI baseline we have to beat), `samsa`, `krafka`,
`kacrab`, `kafkit-client`, plus this crate. Nobody has an honest shared-broker
librdkafka bench yet — that bench is the finish line (see BENCH.md).
