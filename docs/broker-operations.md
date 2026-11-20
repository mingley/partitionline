# Broker operator setup

The local setup workflow initializes, configures, backs up, and restores an
admin journal. It uses a temporary directory and starts no listener. For
single-node Kafka serving, see the
[broker README](https://github.com/mingley/partitionline/blob/e7c6e3cca0deb18e406ac4da4e4ae899834993e5/partitionline-broker/README.md).

## Build

Run these commands from the broker crate directory, either in the repository
or in an extracted archive.

```sh
cargo build --locked --bin operator
./target/debug/operator init --data ./broker-admin --cluster cluster-1 --voter 1
./target/debug/operator status --data ./broker-admin
```

Add `--all-features` to the build to include the optional broker components.

## Configure

| Setting | Example | Limit |
| --- | --- | --- |
| Cluster ID | `cluster-1` | Fixed for this workflow |
| Initial voter | `1` | Fixed for this workflow |
| Log directory name | `data-a` | At most 8 registered names |
| Feature | `metadata.version` | At most 8 features |
| Feature request version | 0 or 1 | Version 2 is rejected |
| Replicas | 1 | At most 16 per assignment |

## Workflow

From the repository root, run `sh scripts/broker-package.sh`. It creates and
checks the crate archive, then installs the operator from the extracted
archive with default and all features. Each installed binary initializes an
admin journal, registers `data-a`, assigns `orders-0`, and updates
`metadata.version` from 2 to 3. The downgrade check confirms that a rejected
write leaves the journal bytes unchanged. Backup and restore preserve those
bytes and the reported feature and assignment.

The archive, file inventory, checksums, toolchain versions, and command logs
are written to `target/broker-package`. The CI check uses Linux x86_64 on the
latest stable Rust.

Backup and restore here cover the admin journal. For Kafka serving, the
catalog and partition journals need a separate backup workflow.

## Capacity

The operator's admin journal uses entries of up to 8,192 bytes. The table
above describes the configured assignment, directory, and feature limits.
Serving capacity depends on separate listener and storage settings.
