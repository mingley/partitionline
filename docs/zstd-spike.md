# Pure-Rust zstd spike (WP-6.2)

**Question:** Can partitionline speak Kafka zstd compression without linking
`libzstd` / `zstd-sys`?

## Kafka wire expectation

Kafka record batches advertise compression type `zstd` (attributes bits).
Brokers and librdkafka decode with the reference C library (`libzstd`).
Frame format is standard zstd, not a Kafka-specific framing layer (unlike
snappy-java on produce).

## Pure-Rust options (2026 survey)

| Crate | Status vs Kafka |
|---|---|
| `ruzstd` | Pure Rust decoder; encoder incomplete / not production-default for Kafka interop |
| `zstd` (crates.io) | Wraps `zstd-sys` → **C** |
| Research codecs | Not Kafka-ecosystem validated at scale |

## Verdict

**Not feasible for default features today** without accepting either:

1. a C dependency (`zstd-sys`), which violates the no-C default rule, or
2. a pure-Rust codec that is not yet the Kafka ecosystem interchange default.

## Recommended path

- Keep zstd **out of default features** (`docs/gaps.md`: blocked on C).
- If demand appears post-crates.io (WP-6.1 survey), offer an **opt-in**
  feature `zstd` that documents the C link, never enable it by default.
- Revisit pure-Rust when a Kafka-compatible encoder/decoder is proven
  against Apache Kafka 3.9/4.x produce+fetch roundtrips in CI.

## Non-goals of this spike

- Do not vendor `libzstd`.
- Do not claim zstd support in README until a feature ships.
