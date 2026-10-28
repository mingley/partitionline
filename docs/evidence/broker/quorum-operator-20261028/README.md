# Metadata-only quorum candidate

This candidate runs three fixed, loopback-only controllers using the broker's
private Raft transport and a metadata-only Kafka frontend. The frontend maps
controller IDs to client endpoints, reports partitions without a data leader
or replicas, and advertises only Metadata, ApiVersions, CreateTopics, and
DeleteTopics. The private transport does not authenticate peers. This is not
Kafka KRaft interoperability, a replicated data plane, a production cluster,
or a performance result.

The sparse worktree was based on commit `99868cad0ecab5e9664427096e0cada4c9ac6037`
with uncommitted changes. Source inputs and test binaries are recorded in the
run receipts. The source snapshot covers the broker source, selected tests and
fixtures, and the root Cargo files, Rust source, and examples; it is not the
complete tracked repository. The receipts hash the sparse broker tree, root
source and examples, `tests/conformance/broker`, and three root Cargo/toolchain
files; they do not hash every repository file, Cargo registry dependency,
configuration file, or environment variable. The final source passed latest-stable Rust 1.99.1
`cargo check`, the existing metadata suite (8 tests), metadata quorum suite
(2 tests), a three-process operator test, and strict Clippy for the operator
and process test. The process test passed three times in all, including twice
before a final documentation-only edit. The final test covers API advertisement,
endpoint mapping, majority create, leader failover, replay after restart,
minority no-ack, delete, identity/locking and listener release.

An independent Apache Kafka 4.1.2 Java Admin client ran twelve successful
operations against the source-hashed operator: creation through a follower,
reads on every controller, leader shutdown and failover, creation, restart and
catch-up, deletion, and absence checks on every controller. The operator build
receipt records its executable hash, copied while the build slot was held.
All controllers shut down and joined their accepted client connections. This
is one graceful local metadata history, not a partition, crash, full API or
production qualification. `attempt-01` retained the initial failure caused by
the copied operator file lacking executable permission; `attempt-02` passed.
The Java jar pins and raw output are retained. Jars came from the same pinned
Apache image as the earlier RF1 peer evidence.

The early checks failed because the sparse worktree omitted the parent crate's
source/examples; the first metadata regression attempt lacked the root
conformance fixture. Those failures are retained. The first strict Clippy run
reported synchronous-test blocking method lints; the final run passed with
function-scoped expectations justified by the synchronous process test.
Several earlier successful test binaries are not archived; the final test
executables and peer operator binary are retained. Rebuild and repeat from a
committed integrated candidate before using this as an exact-source gate.

The SHA-256 inventory verifies stored and reconstructed files. From the main
repository root run:

```
python3 benchmarks/runtime/verify-evidence-archive.py /workspace/work/quorum-profile-evidence
```
