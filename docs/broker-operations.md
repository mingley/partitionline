# Broker operator setup

This page describes the local operator binary built from `partitionline-broker`.
The binary updates an admin journal on disk. It does not open a listen socket,
does not join a cluster, and does not deploy itself.

The workflow below was run on Linux x86_64 with the repository's locked Rust
toolchain. Other platforms are not claimed.

## Build

From the repository root:

```sh
cargo build --locked --offline --manifest-path partitionline-broker/Cargo.toml --bin operator
```

The binary is `partitionline-broker/target/debug/operator`.

## Configure

The example uses these tested values:

| Setting | Value | Limit |
| --- | --- | --- |
| Cluster id | `cluster-1` | Fixed for this example |
| Initial voter | `1` | Broker id must be positive |
| Log directory name | `data-a` | At most 8 registered names |
| Feature | `metadata.version` | At most 8 features |
| Spoken feature versions | 0 and 1 | Version 2 is rejected |
| Replicas | 16 | Assignment cap in this binary |

A lower feature level than the stored level is rejected. The journal is left
unchanged. There is no automatic rollback process; the previous level remains
because the write did not commit.

## Workflow

`scripts/broker-package.sh` runs these steps in a temporary directory:

1. `init` creates `admin.journal` for cluster `cluster-1`.
2. `register-dir` records log directory `data-a`.
3. `assign` places partition `orders-0` on broker 1 in that directory.
4. `feature` sets `metadata.version` to 2 with version 0, then to 3 with version 1.
5. A downgrade to level 1 exits non-zero and leaves level 3.
6. `backup` copies `admin.journal`.
7. `restore` copies that file into a new data directory.
8. `status` on the restored directory prints level 3 and log directory `data-a`.

Observe with:

```sh
operator status --data DIR
```

Backup and restore copy only `admin.journal`. They do not copy a Kafka log
directory tree, because this binary does not store partition bytes.

## Capacity

The operator binary uses journal entries up to 8192 bytes, 16 replicas, 8 log
directory names, and 8 feature names. Those are the caps exercised by the
package script. They are not a production capacity plan.
