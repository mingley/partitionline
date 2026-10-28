# Broker implementation

This page is the broker half of the client-and-broker program. KL01–KL10
are the client. KL11 is the broker. [tasks.json](tasks.json) is the only
status list. Finished work is pushed to `main`.

The earlier client-only scope does not drop broker and control-plane APIs.
Existing client tasks and evidence stay. Finish one ready card at a time,
and leave a card that already has an owner. Split a large subsystem into
dependent children before implementing it. A measured rejected optimization
is a completed study. It does not implement the rejected behavior, and it
does not prove performance leadership.

## Crate and protocol boundary

`partitionline-broker` is an unpublished crate built with latest stable Rust.
It is excluded from the root workspace and client package. Its dependency graph
is separate; backend and authentication changes follow the dependency policy.

The initial server feature matrix is
`tests/conformance/broker/features.json`. Its API keys 0–92 form a classification
envelope, not verified version ranges. KL11-04 must pin the exact Apache source
and complete/adjust that envelope for target releases 4.1.2, 4.2.1 and 4.3.1.
Every current data/control/coordinator/security/admin API needs a disposition and
versioned independent evidence. Retired/legacy APIs require an explicit upstream
reason; they cannot be silently counted as implemented. Advertise only actual,
tested handlers. Protocol unsupported/error responses must follow the pinned
wire contract, never fabricated success.

## First independent implementation contracts

KL11-01 through KL11-09 are done. The paragraphs below are the contracts
those cards implemented. They are not the next task. Current status is
[tasks.json](tasks.json). The production bar further down is still open.

KL11-02 created `src/transport.rs` and transport integration tests. Use Tokio
framed I/O with positive configured limits for connections, request/response
bytes and absolute read/handler/write deadlines. Bound active tasks; cap length
before allocation; preserve per-connection response order; join/drop sockets and
tasks during cancellation/shutdown. Provide an asynchronous handler interface
that can later await storage without blocking the executor. Module exports and
shared manifest edits are coordinated by the main agent.

KL11-03 created `src/journal.rs` and journal integration tests. Use an append-only
checksummed entry format with checked monotonic logical offsets and an explicit
record count. A durable append synchronizes data before reporting success.
Caller-supplied paths are configuration, not unchecked topic names. Bound entry
size, total file bytes, recovery/index memory and fetch output. A demonstrably
incomplete final entry can be truncated on recovery with a reported outcome;
interior CRC/length/offset corruption fails closed. Failed/partial writes must
not silently advance the offset or allow subsequent appends into damaged state.
Recovery scans must not allocate from unchecked lengths or assume a partial read
is EOF. The journal card itself does not validate Kafka record batches.
KL11-06, which is done, appends validated Produce batches to the durable
log. Replication, transaction durability, and a complete broker remain
later work.

These modules own their public errors, configuration, and interfaces. They
do not modify client hot files or impersonate the benchmark null broker.
KL11-04 through KL11-08 added header negotiation, persistent topic metadata,
validated Produce, Fetch, ListOffsets, and single-node interop. Those cards
are done.

## What is still open

The [task registry](tasks.json) is the current list of open cards and their
dependencies. On 28 October 2026 it had 79 KL11 cards: 54 done, six in
progress, and 19 pending. The previous 22 October snapshot counted 63 done
out of 76; several cards have since been reopened because their evidence did
not cover the full stated behavior.

Open work includes broker registration and fencing, partition assignment,
replicated logs and acknowledgments, leader failover, transactional offsets,
classic groups and offsets, quotas, OIDC and SASL reauthentication, and KRaft
directory identity and pre-vote. Mixed-version operation, overload and shutdown,
fuzzing, sustained resource and continuity runs, packaging, and comparative
benchmarks also remain open. See the registry for the individual cards and
their current status.

## Full server behavior

This list describes the lane's scope, not a claim that each behavior is
available in the operator. Some components have tests in isolation, while
their integrated or distributed behavior remains open. The
[broker README](../../partitionline-broker/README.md) describes the current
operator boundaries.

The queue covers segment/index recovery, retention and compaction; durable
metadata quorum election/replication/snapshots and broker fencing; replica
catch-up, ISR/high watermarks and failover; idempotent producers and transactional
markers/LSO/offset atomicity; classic/cooperative/static/modern consumer groups,
share and streams coordinator protocols; TLS/mTLS, SASL, validating OIDC,
optional reviewed GSSAPI, ACLs and delegation tokens; quotas, telemetry, full
administration and tiered logs.

Each implementation needs failing behavioral/fault cases and pinned independent
wire/semantic evidence where relevant. Persistence tests include restart, torn
writes and corrupt bytes. Distributed histories include partitions, stale
epochs, quorum loss and lost acknowledgments. Acks, fsync, replication visibility
and ambiguous outcomes are explicit; no durability guarantee is inferred from
matching record totals.

## Production and leadership completion

Production needs complete independent API/version/profile matrices; at least
1,000 seeded faults per supported profile; bounded overload and shutdown; current
mixed-version upgrade/security histories; sustained fuzz; 24-hour resource and
seven-day operational continuity evidence; verified packaging/docs, backup,
restore and rollback. Running or launched campaigns remain unfinished until
their actual duration and acceptance criteria pass. Preserve failed attempts
and minimized regressions. Existing Suite HOLD and release/signoff rules remain.

Client and server comparisons use pinned relevant competitors on equal hardware,
payloads, durability, security and offered load. Report verified useful work,
CPU/RSS/disk and tail latency; retain every losing cell and confidence interval.
The current authorized campaign uses the shared x86_64 workspace and no
additional benchmark machines or external operator. Its results are local,
workload-specific engineering evidence. The frozen
[benchmark contract](../benchmark-contract.md) still requires independent
reproduction for profile claims and both x86_64 and arm64 for a global client
claim. Profile leadership needs superiority in every required named cell. A
finite benchmark set establishes a scoped result, not a universal claim over
every possible machine, workload, client and server.

KL11-56 is the final fail-closed audit and depends on the entire existing client
queue plus the server completion chain. It cannot close with missing required
features, unresolved correctness failures, unexecuted operational evidence or
unsupported performance claims. Publishing/deploying, paid infrastructure,
external messages and live traffic require their own authorization; implementing
and testing the goal does not silently perform those operations.

