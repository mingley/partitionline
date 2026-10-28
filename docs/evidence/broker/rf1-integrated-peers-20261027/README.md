# Integrated RF1 server: independent Java and librdkafka histories

Two independent clients exercised an ordinary plaintext RF1 server executable
on Linux x86_64, each with a fresh listener and data directory. Both completed
and cleanly shut down their server processes. The executed binary is included
and has SHA-256 `46638c7015988797b85bd1fb857b01057f8d44747e411e15871eb1db3f70480b`.

The earlier `integrated-rf1-01` test built and exercised the operator but did
not capture that operator executable's hash at execution time or bind its
manifests and fixtures. This run copied and hashed the executable after that
test. The original build receipt's broker Rust source hashes match commit
`c3b17722a76d3ddb5689811c55ce53cb5d64be9a`, and all 5,924 tracked broker and
workspace inputs in the included commit archive matched the worktree before
and after the histories. These comparisons do not prove the post-build
executable was built from that commit. A source-bound build and a fresh repeat
are required to establish that link. The source verification code, receipts,
build log and input archive are included.

The Apache Kafka 4.1.2 SDK's Admin created a two-partition topic; its Producer
used `acks=1`, `enable.idempotence=false` and no compression. Its manually
assigned Consumer checked record offsets, keys, values, UTF-8, nulls and
ordered duplicate/null/empty headers. It read six records after a graceful
restart, wrote two further records and read all eight. Each server shutdown
joined all six accepted connections without worker failures.

The librdkafka 2.15.0 client independently created a separate two-partition
topic, produced 24 records using `acks=1`, disabled idempotence and no
compression, and checked all records, headers and timestamps with manual
Fetch. It verified low/high watermark and timestamp ListOffsets before and
after graceful restart. Server shutdown joined all connections without worker
failures. Both peer sources, binary dependencies, complete outputs and journal
bytes are included. Peer compilation and dependency provenance, including the
initial javac and C compiler setup errors, remain in the earlier retained RF1
peer archives; this archive includes their compile logs and build manifests.

These are small graceful-restart histories; they do not qualify replication,
idempotence, transactions, groups, security, crash recovery, sustained load,
performance or production readiness. They do not establish a source-bound
pass for the integrated commit.

Verify archive storage and reconstructed bytes with `python3
benchmarks/runtime/verify-evidence-archive.py <this directory>`.
