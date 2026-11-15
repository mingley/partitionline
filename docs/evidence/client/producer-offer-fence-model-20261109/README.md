# Producer offer and identity fence model

This is an isolated state-machine experiment, not a change to the producer.
It explores how immutable sequenced batches, ambiguous network offers and
identity transitions might be coordinated. The code is not linked to real
admission, transport, responses, callbacks or transactions. It is not a
production delivery fix.

## Scope

The model reserves all partitions in a frame together, retains its identity,
sequences and bytes after an ambiguous offer, and permits only one active batch
per partition. It registers an offer before the first possible socket poll.
Before a retry or identity transition, it requires caller attestations that
the relevant transport actors and response dispatchers have stopped and
joined. These attestations are assumptions, not proof about real transport.
Joining a task does not prove the broker did not append. A retry retains the
same identity and batch on a newer session; an identity transition waits for
definitive outcomes and retired receipts.

An ambiguous outcome can block draining and an identity change indefinitely.
The journal is in memory and does not establish restart safety. One active
batch per partition can reduce the idealized throughput ceiling compared with
multiple in-flight batches; no performance measurement was made. Logical
journal limits are not process RSS or peak admission-memory limits.

## Checks

The latest isolated source was commit
[`a413106`](https://github.com/mingley/partitionline/tree/a413106121f8032d73efc6bd6ed9acfc800ebed8/experiments/producer-offer-journal).
Its [run](https://github.com/mingley/partitionline/actions/runs/42699289246) passed 12 model
tests and Clippy, but failed formatting. The overall workflow failed.
Earlier iterations passed nine and 11 model tests respectively; the
[first run](https://github.com/mingley/partitionline/actions/runs/42698134289) failed
formatting and Clippy and the
[second run](https://github.com/mingley/partitionline/actions/runs/42698690676) failed
formatting. Passing these model controls does not establish the corresponding
behavior in the client.

Real transport fencing, partial writes and responses, protocol error handling,
identity-changing RPCs, topic incarnation, transactions, acks=0, durable
receipts and crash recovery remain unimplemented or unqualified. No real Kafka
compatibility or performance test was run.

The exact [model source](model.rs.txt), [manifest](Cargo.toml.txt),
[lockfile](Cargo.lock.txt), [model notes](model-notes.md.txt), and
[workflow](workflow.yml.txt) are retained here as evidence; they are not
workspace crates or active CI.
