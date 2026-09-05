# Companion crate design: `partitionline-schema` (WP-6.3)

**Status:** design only. Core `partitionline` `0.1.0` is on crates.io — the
publish gate for starting this companion is met. Build/publish the companion
only when survey [#85](https://github.com/mingley/partitionline/issues/85)
(or equivalent adopter demand) justifies it. Schema Registry support stays out
of the core client (`docs/gaps.md`).

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
2. Default features: no C, no OpenSSL (rustls).
3. Round-trip example against a local Schema Registry + Kafka.
4. Documented as optional in `docs/ADOPTION.md` and core README.

## Trigger

Ship after:

1. `partitionline` on crates.io
2. Adoption survey [#85](https://github.com/mingley/partitionline/issues/85)
   shows Schema Registry as a real pilot blocker (or owner prioritizes it)
