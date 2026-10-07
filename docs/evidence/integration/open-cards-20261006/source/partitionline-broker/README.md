# partitionline-broker

Experimental Kafka broker components. This crate is under development and is
not published (`publish = false`).

The crate includes:

- TCP request framing, connection limits, and shutdown handling.
- Metadata, topic creation/deletion, Produce, Fetch, and ListOffsets handlers.
- Checksummed journals, segmented partition logs, retention, and compaction.
- Metadata quorum state and Raft replication components.
- Optional TLS, SASL, OIDC, and compression support.

Private Raft peer sockets use TCP_NODELAY so small election and heartbeat
frames do not wait for TCP delayed acknowledgments. Peer deadlines and queue,
frame and task limits still apply.

Available handlers and API versions depend on the configured router and features.
Replication, coordinator behavior, compatibility, and operational testing remain
in progress. See the [broker plan](../docs/plan/broker-implementation.md) and
[KL11 tasks](../docs/plan/tasks.json) for their status and acceptance criteria.

Build and test from the repository root:

```sh
cargo check --locked --manifest-path partitionline-broker/Cargo.toml
cargo test --locked --manifest-path partitionline-broker/Cargo.toml
```

The [null broker](../benchmarks/nullbroker) is a benchmark tool with synthetic
responses. Use the storage and replication components in this crate when working
on broker behavior. Production and comparative performance qualification are
still pending.
