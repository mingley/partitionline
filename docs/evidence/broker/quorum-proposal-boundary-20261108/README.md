# Proposal admission during leader removal

The broker's single-owner Raft node appends a data proposal while a change
removing that leader is still uncommitted. In the exercised three-voter
schedule, the proposal is committed with the removal, the node steps down,
and its next proposal returns `NotLeader` without changing the WAL. This is a
test of that particular schedule, not a proof that every Kafka error 41 is
returned before an append.

The test-only source was qualified on x86_64 with stable Rust 1.99.1 in
[run 42453459064](https://github.com/mingley/partitionline/actions/runs/42453459064).
The new test and two existing leader-fencing controls passed individually in
both default and all-feature configurations; formatting and strict
all-target, all-feature Clippy also passed. The qualification tree included a
temporary mutation script and workflow, and used these exact broker source
bytes:

- `partitionline-broker/tests/raft_membership.rs` SHA-256
  `dc6e09d5f5b7bea32aa85d151502a65021480fd18cbf54c7d90c3feebe0d2ffd`
- `partitionline-broker/src/raft/replication.rs` SHA-256
  `f74747a0e7c9c4e211388a9428b0eaf1c5773f9c41c942b9ab72c2d4132c573e`

In a separate negative control, the qualification script deliberately returned
`NotLeader` immediately after a real append. The new test detected the changed
log position in both feature configurations. This mutation checks the test's
sensitivity; it does not show that production can reach that error path.

The earlier [run 42453177458](https://github.com/mingley/partitionline/actions/runs/42453177458)
passed the selected tests and Clippy but failed its formatting gate. It is
retained as a failed qualification run. Neither run explains the ambiguous
Delete error 56 in
[main CI run 42452041729](https://github.com/mingley/partitionline/actions/runs/42452041729);
that failure remains open. The original Delete was not retried.
