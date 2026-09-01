# Config

`ProducerConfig`, `ConsumerConfig`, and `AdminConfig` are chainable
builders. Raw fields stay writable.

```rust,no_run
use std::time::Duration;
use partitionline::{Acks, Compression, IsolationLevel, ProducerConfig, Sasl};

let _cfg = ProducerConfig::bootstrap(["127.0.0.1:9092"])
    .acks(Acks::All)
    .linger(Duration::from_millis(5))
    .compression(Compression::Lz4)
    .sasl(Sasl::scram_sha256("alice", "secret"));
let _iso = IsolationLevel::ReadCommitted;
```

`connect_timeout` is on all three builders. Interceptors:
`ProducerConfig::interceptor` / `ConsumerConfig::interceptor`
(`close`; consumer also sees `on_commit`).

## TLS

`TlsConfig` on the same builders. Handshake is `rustls` + `ring` (not
OpenSSL). Custom CA PEM, or Mozilla roots if `ca_pem` is omitted.
Optional client cert/key for mTLS. SNI defaults to the bootstrap host.

## SASL

| Helper | Mechanism |
|---|---|
| `Sasl::plain` | PLAIN |
| `Sasl::scram_sha256` / `scram_sha512` | SCRAM (pure Rust) |
| `Sasl::oauthbearer` | unsecured JWT (`alg=none`), same as librdkafka `enable.sasl.oauthbearer.unsecure.jwt` |
| `Sasl::oidc` | RFC 6749 client_credentials, then OAUTHBEARER |

**GSSAPI / Kerberos is not implemented** (librdkafka uses Cyrus SASL).

## Defaults that differ from Java

| Knob | This crate | Java |
|---|---|---|
| `allow.auto.create.topics` | `false` | consumer `true` |
| `delivery.timeout.ms` | 30s | 120s |
| `max.block.ms` | 30s | 60s |
| `auto.offset.reset` | earliest | latest |
| `enable.auto.commit` | off | on |
| `fetch.max.bytes` / `max.partition.fetch.bytes` | 16 MiB / 16 MiB | 50 MiB / 1 MiB |
| `heartbeat.interval.ms` | 150 ms | 3 s |
| `connections.max.idle.ms` of `0` | never close | Java 0 closes immediately |

Same as Java unless noted: `buffer.memory` 32 MiB, `max.request.size` 1
MiB, `retry.backoff.ms` 100 / max 1s, `reconnect.backoff.ms` 50 / max 1s,
`connections.max.idle.ms` 9 minutes, `transaction.timeout.ms` 60s,
`metadata.max.age.ms` 5 minutes.

Zero `buffer.memory` / `max.request.size` means no extra cap (batch size
still applies). An oversized record returns `Error::RecordTooLarge`.
