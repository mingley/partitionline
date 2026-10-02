# partitionline-broker

Rust Kafka broker development crate. The full implementation and qualification
queue is KL11 in `../docs/plan/tasks.json`; its contract is
`../docs/plan/broker-implementation.md`.

This is an experimental crate with `publish = false`. The crate now provides
bounded framed TCP transport with optional replies and joined shutdown, plus a
checksummed durable journal with bounded fetch/recovery and torn-tail reporting.
It has no Kafka request handlers yet. Versioned handlers are separate tasks,
followed by replication/coordinators/security, independent interop and
operational gates.

The benchmark `nullbroker` is a validation tool and does not supply production
storage or replication. A successful unit test does not qualify this broker.

```sh
cargo check --locked --manifest-path partitionline-broker/Cargo.toml
```

