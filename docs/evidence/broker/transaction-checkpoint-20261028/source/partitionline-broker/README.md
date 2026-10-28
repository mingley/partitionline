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
port. `--sync-concurrency` sets the maximum concurrent partition syncs per
storage actor (1–64, default 4); measure the setting on your storage.

The other operator commands configure a separate on-disk admin journal. Their
backup and restore commands cover that admin journal only; they do not back up
the listener's catalog or partition journals. See
[broker operations](../docs/broker-operations.md).

The experimental consumer, share and streams group coordinators expose
`describe` as `Result<Option<_>, Error>`. Callers must handle `Error::Poisoned`
after an uncertain storage write and reopen the coordinator to recover. The
classic coordinator similarly returns `Result` from `epoch`, `generation`, and
`assignment`; reopen it after `Error::Poisoned`. Classic snapshot checkpoints
reserve the adjacent `.checkpoint.tmp` pathname. The
classic offset store similarly returns `Result` from `list`, `describe`, and
`fetch`; handle `Error::Poisoned` by reopening the store. Its journal rolls
over to a complete checkpoint when it reaches a configured history limit.
The checkpoint format supports up to 65,535 groups and 65,535 offsets per group;
larger configured limits are rejected. The lifecycle coordinator returns
`Result` from `owned`, and both `owned` and `commit` report `Error::Poisoned`
after persistence fails. Reopen the lifecycle store to recover. The transaction
coordinator returns `Result` from `epoch` and `state` and must likewise be
reopened after a persistence error. Its checkpoints must fit in one configured
journal entry, file and fetch budget, and reserve the adjacent `.checkpoint.tmp`
pathname. A legacy event-only journal whose state cannot fit a checkpoint stays
readable and can append events until its configured history, index or file limit
is reached. Those appends can grow state beyond the 64 MiB entry maximum, after
which increasing limits cannot enable single-entry rollover. A multi-entry format
is not yet implemented. Once a new checkpoint is written, older binaries cannot
read the journal. Transaction
marker propagation and fencing integration are not implemented: initializing
an ongoing transaction currently clears its stored partition set. Checkpoint and
recovery allocations are not bounded as total process memory.

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

The experimental metadata-only quorum mode runs three fixed controllers on
loopback addresses. It uses a private protocol, does not authenticate peers,
and must not be exposed to an untrusted network. Each member uses the same
`--members` list with entries `ID@DIRECTORY_HEX@PRIVATE_ADDR@CLIENT_ADDR` and
its own `--id` and `--data` values. The directory ID is 32 nonzero hexadecimal
characters. For example, run one command per member using this shared list:

```sh
members='0@00000000000000000000000000000001@127.0.0.1:19100@127.0.0.1:19200,1@00000000000000000000000000000002@127.0.0.1:19101@127.0.0.1:19201,2@00000000000000000000000000000003@127.0.0.1:19102@127.0.0.1:19202'
cargo run --manifest-path partitionline-broker/Cargo.toml --bin operator -- \
  serve-quorum --data ./quorum-0 --cluster local-quorum --id 0 --members "$members"
```

Run the same command for IDs 1 and 2 with `./quorum-1` and `./quorum-2`.
Controller endpoints are advertised to metadata clients; partitions have no
leader or replicas, and Produce and Fetch are unavailable. The configured
limits retain at most 64 metadata positions and 1 MiB of source payload; the
mode does not automatically prune that history. This mode does not run a
replicated Kafka data plane or interoperate with Kafka KRaft.
