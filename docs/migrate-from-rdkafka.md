# Migrate from rust-rdkafka / librdkafka

partitionline is **not** a drop-in for `rd_kafka_*` or rust-rdkafka types.
It is a pure-Rust client with Java-shaped builders. Use this map when porting
a service.

## Dependency

```toml
# before
rdkafka = { version = "0.39", features = ["cmake-build"] }

# after (once published)
partitionline = "0.1"

# until crates.io publish — pin a tag, not floating main
partitionline = { git = "https://github.com/mingley/partitionline", tag = "v0.1.0-rc.4" }
```

No C toolchain, librdkafka, OpenSSL, or cmake-build feature required for the
default feature set.

## Concepts

| librdkafka / rust-rdkafka | partitionline |
|---|---|
| `FutureProducer` / `BaseProducer` | `Producer` |
| `StreamConsumer` / `BaseConsumer` | `Consumer` (manual) or `ConsumerGroup` / `ShareGroup` |
| `AdminClient` | `Admin` |
| String property bag (`ClientConfig::set`) | Typed builders: `ProducerConfig`, `ConsumerConfig`, `AdminConfig` |
| `Message` | `FetchedRecord` / `ShareRecord` / `ProduceRecord` |
| Delivery callbacks | `send` / `send_all` futures, or `try_send` + `flush` |
| `commit` / committed offsets | `commit_with_metadata`, `committed`, `OffsetAndMetadata` |

## Config map (common)

| librdkafka | partitionline |
|---|---|
| `bootstrap.servers` | `ProducerConfig::bootstrap` / `ConsumerConfig::bootstrap` |
| `acks` | `ProducerConfig::acks` (`Acks`) |
| `linger.ms` | `ProducerConfig::linger` (`Duration`) |
| `compression.codec` | `ProducerConfig::compression` (`Compression`; gzip/snappy/lz4; **no zstd** in default features) |
| `enable.idempotence` | Implied by idempotent settings / transactions; transactional id implies idempotence |
| `transactional.id` | `ProducerConfig::transactional_id` |
| `group.id` | `ConsumerGroup::join_*` group id argument |
| `auto.offset.reset` | `ConsumerConfig::auto_offset_reset` (**default Earliest**, Java/librdkafka often latest) |
| `enable.auto.commit` | Off by default; commit explicitly |
| `isolation.level` | `ConsumerConfig::isolation` (`IsolationLevel`) |
| `security.protocol` / SSL | `TlsConfig` on the config builder (rustls, not OpenSSL) |
| `sasl.mechanism` PLAIN/SCRAM/OAUTHBEARER | `Sasl::…` / OIDC helpers |
| `sasl.mechanism` GSSAPI | **Not available** (blocked on C; see `gaps.md`) |
| `client.rack` | `ConsumerConfig::rack` |

## API map

| Task | rust-rdkafka-ish | partitionline |
|---|---|---|
| Produce one | `send` + delivery future | `Producer::send` |
| High throughput | poll delivery queue | `try_send` + `flush` |
| Assign partitions | `assign` | `Consumer::assign` / `assign_topic` / `assign_partitions` |
| Subscribe group | `subscribe` | `ConsumerGroup::join_topics` (or sticky / cooperative / KIP-848 variants) |
| Poll records | `recv` / `poll` | `fetch` or `group.poll` → `ConsumerRecords` |
| Commit | `commit_message` | `commit_with_metadata(recs.next_offsets())` |
| Transactions | init / begin / send_offsets / commit | same names on `Producer`; see `examples/eos.rs` |
| Create topic | admin create | `Admin::create_topics` |

## Intentional differences

1. **No C** — zstd and Kerberos stay out of default features.
2. **Defaults** — earlier offset reset, no auto topic create on consumers,
   shorter delivery / max.block timeouts (README table).
3. **Types over strings** — invalid configs fail at compile/type time where
   possible.
4. **Not symbol-compatible** — rewrite call sites; do not expect
   `BorrowedMessage` lifetimes.

## Validation checklist

1. Point at a test cluster; run `cargo run --release --example roundtrip`.
2. Port produce path; confirm offsets and partitioning (murmur2 default).
3. Port consumer group; confirm `auto.offset.reset` behavior matches product
   intent (Earliest vs latest).
4. If you used zstd or GSSAPI, plan compression/auth alternatives first.
5. Compare metrics via `Producer::metrics` / `Consumer::metrics` during soak.

## Further reading

- [`guide.md`](guide.md) — operator tour
- [`gaps.md`](gaps.md) — full capability inventory
- [`security.md`](security.md) — threat model
