# partitionline-schema

Scaffold companion for Confluent-compatible **wire framing** used with
[`partitionline`](https://crates.io/crates/partitionline).

**Not published** (`publish = false`). Workspace-excluded from the core
`partitionline` crate package. Default features: **no C, no OpenSSL**.

## Now

- `encode` / `decode`: magic byte `0` + big-endian schema id + payload

## Later (demand-gated)

HTTP Schema Registry client + Avro / Protobuf / JSON codecs — see
[`docs/schema-companion.md`](../docs/schema-companion.md) and survey
[#85](https://github.com/mingley/partitionline/issues/85).

```bash
cargo test --manifest-path partitionline-schema/Cargo.toml
```
