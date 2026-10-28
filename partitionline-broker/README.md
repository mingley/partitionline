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

The operator can run an ordinary single-node listener:

```sh
cargo run --manifest-path partitionline-broker/Cargo.toml --bin operator -- \
  serve --data ./broker-data --listen 127.0.0.1:9092 \
  --advertise-host 127.0.0.1 --cluster local-cluster --broker-id 0
```

The listener is plaintext. Its catalog and partition journals are synchronized
locally before successful acknowledgments. Its replication factor is one: it
does not support replicated data, transactions, idempotent producers or consumer
groups. It advertises only its implemented APIs. `SIGINT` and `SIGTERM` stop
the listener and join its storage actor. Only one `serve` process can use a
data directory at a time; other programs must not write its files while the
server runs. The directory must retain the same cluster and broker identity on
restart. Use
`--advertise-port` if clients must connect to a port different from the bound
port.

The other operator commands configure a separate on-disk admin journal. Their
backup and restore commands cover that admin journal only; they do not back up
the listener's catalog or partition journals. See
[broker operations](../docs/broker-operations.md).

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
