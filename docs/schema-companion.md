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
