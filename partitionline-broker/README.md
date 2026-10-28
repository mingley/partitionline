# partitionline-broker

Experimental Kafka broker components. This crate is under development and is
not published (`publish = false`).

The crate includes:

- TCP request framing, connection limits, and shutdown handling.
- Metadata, topic creation/deletion, Produce, Fetch, and ListOffsets handlers.
- Checksummed journals, segmented partition logs, retention, and compaction.
- Metadata quorum state and Raft replication components.
- Optional TLS, SASL, OIDC, and compression support. SASL session
  renewal stays off until a listener calls `with_reauthentication`.
  The rules are in [SASL reauthentication](../docs/sasl-reauthentication.md).

Available handlers and API versions depend on the configured router and
features. OIDC token validation, connection reauthentication, Kerberos,
KRaft directory identity, pre-vote, and sustained qualification remain open.
The [broker plan](../docs/plan/broker-implementation.md) tracks the work.
This crate is not a production cluster.

The operator binary configures an on-disk admin journal. It does not listen or
deploy a cluster. See [broker operations](../docs/broker-operations.md).

```sh
sh scripts/broker-package.sh
```

Build and test from the repository root:

```sh
cargo check --locked --manifest-path partitionline-broker/Cargo.toml
cargo test --locked --manifest-path partitionline-broker/Cargo.toml
```

The [null broker](../benchmarks/nullbroker) is a benchmark tool with synthetic
responses. Use the storage and replication components in this crate when working
on broker behavior. Production and comparative performance qualification are
still pending.
