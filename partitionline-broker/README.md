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

Private Raft peer sockets use TCP_NODELAY so small election and heartbeat
frames do not wait for TCP delayed acknowledgments. Peer deadlines and queue,
frame and task limits still apply.

Available handlers and API versions depend on the configured router and
features. Of the 76 KL11 cards, 63 are done. That includes framed transport,
validated Produce and Fetch, retention, compaction, metadata election, and
failover. Still open: OIDC token validation (KL11-37, in progress), SASL
reauthentication on a broker connection (KL11-71, in progress), optional
Kerberos, KRaft directory identity and pre-vote, and the qualification runs
(fuzz, benchmarks, a 24-hour resource run, and a 7-day run). The
[broker plan](../docs/plan/broker-implementation.md) lists each open card.
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
